import json
import urllib.parse
import logging
import os
import csv
import io
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Any

if Path(".env").exists():  # .env check for local execution
    load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL"))
logger = logging.getLogger(__name__)


def generate_response(status_code: int, message: str, body: Dict[Any, Any] = None):
    response = {
        "StatusCode": status_code,
        "Message": message,
        "Header": {"Content-Type": "application/json"},
        "Body": json.dumps(body),
    }
    return response


def format_and_filter_targets(s3_event: Dict[str, Any]) -> List[Dict[str, str]]:
    logger.info("formatting and filtering tagets...")

    res = []

    for record in s3_event["Records"]:  # iterate on s3 event records and append to res
        event_type: str = record["eventName"]
        bucket_name: str = record["s3"]["bucket"]["name"]
        s3_object_key: str = record["s3"]["object"]["key"].split("/")
        principal_id: str = record["userIdentity"]["principalId"]

        prefix = "/".join(s3_object_key[:-1])  # get s3 object prefix
        key = urllib.parse.unquote(s3_object_key[-1])  # get object key

        if (
            "s3" in record
            and prefix == "batch/send"
            and key.endswith(".csv")
            and event_type.startswith("ObjectCreated")
        ):
            res.append(
                {
                    "BucketName": bucket_name,
                    "Prefix": prefix,
                    "Key": key,
                    "PrincipalId": principal_id,
                    "Timestamp": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
                }
            )

    return res


# reads a CSV file in batches.
def batch_read_csv(file_obj, batch_size: int):
    batch, row_errors = [], []
    csv_reader = csv.DictReader(file_obj)

    for row_number, row in enumerate(csv_reader, start=2):
        if not row:  # Skip empty rows
            continue

        try:
            validate_result = validate_row(row)

            custom_fields = {"row_number": row_number}

            row_info = OrderedDict({**custom_fields, **row})

            if validate_result["IsValid"]:
                batch.append(row_info)
            else:
                row_errors.append(
                    OrderedDict(
                        {
                            **row_info,
                            "Error": "Missing required fields",
                            "MissingFields": validate_result["MissingFields"],
                        }
                    )
                )
        except Exception as e:
            row_errors.append(OrderedDict({**row_info, "Error": "Unidentified error"}))
            continue

        if len(batch) == batch_size:
            yield batch, None
            batch = []

    if batch or row_errors:  # Yield batch or row_errors if any
        yield batch, row_errors


def validate_row(row: Dict[str, Any]) -> Dict[str, Any]:  # Validates a single CSV row.
    csv_required_fields = os.getenv("CSV_REQUIRED_FIELDS")

    missing_fields = []

    for field in csv_required_fields.split(","):
        if field not in row or not row[field]:
            missing_fields.append(field)

    return {"IsValid": not missing_fields, "MissingFields": missing_fields}


def generate_csv(headers: List[str], contents: List[Dict[str, Any]]) -> str:
    # Generate CSV content in memory
    try:
        output = io.StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=headers)
        csv_writer.writeheader()

        for content in contents:
            csv_writer.writerow(content)

        csv_content = output.getvalue()
        output.close()

        return csv_content
    except Exception as e:
        logger.error(f"Error generating csv: {e}")

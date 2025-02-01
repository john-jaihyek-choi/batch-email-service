import json
import urllib.parse
import logging
import os
import csv
import io
import re
from config import config
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Dict, List, Any, Literal
from boto3_helper import get_s3_object, send_sqs_message, get_ddb_item

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


# reads a CSV file in batches.
def batch_read_csv(file_obj, batch_size: int):
    batch, row_errors = [], []
    csv_reader = csv.DictReader(file_obj)
    if csv_reader.fieldnames is None:
        row_errors.append(
            OrderedDict(
                {
                    "Error": "No headers found",
                }
            )
        )
        yield batch, row_errors

    for row_number, row in enumerate(csv_reader, start=2):
        if not row:  # Skip empty rows
            continue

        try:
            custom_fields = {"row_number": row_number}
            row_info = OrderedDict({**custom_fields, **row})

            basic_fields, template_fields = validate_basic_fields(
                row, config.EMAIL_REQUIRED_FIELDS
            ), validate_template_fields(row, config.TEMPLATE_METADATA_TABLE_NAME)

            if (
                basic_fields or template_fields
            ):  # basic or template specific fields missing
                message = f"Missing {"basic" if basic_fields else ""}{" & " if basic_fields and template_fields else ""}{"template specific" if template_fields else ""} required fields"
                row_errors.append(
                    OrderedDict(
                        {
                            **row_info,
                            "Error": message,
                            "MissingFields": basic_fields + template_fields,
                        }
                    )
                )
            else:  # no missing fields
                batch.append(row_info)
        except Exception as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                row_errors.append(
                    OrderedDict({**row_info, "Error": f"Template does not exist {e}"})
                )
            else:
                row_errors.append(
                    OrderedDict({**row_info, "Error": f"Unidentified error {e}"})
                )
            continue

        if len(batch) == batch_size:
            yield batch, []
            batch = []
    else:  # Yield batch and row_errors once completion
        yield batch, row_errors


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


def generate_email_template(
    s3_bucket: str,
    s3_key: str,
    template_type: Literal["html", "txt"],
    target_errors: List[Dict[str, Any]],
) -> str:
    try:
        file = get_s3_object(s3_bucket, s3_key)
        template = file["Body"].read().decode("utf-8")

        attachments, batch_success_details = [], []
        aggregate_success_count, aggregate_error_count = 0, 0

        for target in target_errors:
            file_name = target["Target"].split("/")[-1]

            success_count, error_count = target.get("SuccessCount", 0), target.get(
                "ErrorCount", 0
            )
            total_count = success_count + error_count
            aggregate_success_count += success_count
            aggregate_error_count += error_count

            if template_type == "html":
                batch_success_details.append(
                    f"<li>{file_name} – {error_count} of {total_count} rows failed</li>"
                )

                attachments.append(f"<li><a>{file_name}</a></li>")
            else:
                batch_success_details.append(
                    f"- {error_count} of {total_count} rows failed\n"
                )
                attachments.append(f"- {file_name}\n")

        aggregate_total_count = aggregate_success_count + aggregate_error_count
        aggregate_error_rate = round(
            aggregate_error_count / aggregate_total_count * 100
        )
        aggregate_success_rate = round(
            aggregate_success_count / aggregate_total_count * 100
        )

        replacements = {
            "{{aggregate_success_rate}}": (f"{aggregate_success_rate}"),
            "{{aggregate_error_rate}}": (f"{aggregate_error_rate}"),
            "{{aggregate_success_text}}": (
                f'<div class="bar-success" style="width: {aggregate_success_rate}%">{aggregate_success_rate}%</div>'
                if aggregate_success_rate
                else ""
            ),
            "{{aggregate_error_text}}": (
                f'<div class="bar-failed" style="width: {aggregate_error_rate}%">{aggregate_error_rate}%</div>'
                if aggregate_error_rate
                else ""
            ),
            "{{attachment_list}}": f"{"".join(attachments)}",
            "{{batch_success_details}}": f"{"".join(batch_success_details)}",
        }

        template = re.sub(
            r"{{(aggregate_success_rate|aggregate_error_rate|aggregate_success_text|aggregate_error_text|attachment_list|batch_success_details)}}",
            lambda match: replacements.get(match.group(0), match.group(0)),
            template,
        )
        logger.debug(template)
        return template
    except Exception as e:
        logger.exception(f"Error generating template: {e}")
        return ""


def generate_response(status_code: int, message: str, body: Dict[Any, Any] = None):
    try:
        response = {
            "StatusCode": status_code,
            "Message": message,
            "Header": {"Content-Type": "application/json"},
            "Body": json.dumps(body),
        }
        return response
    except TypeError as e:
        logger.exception(f"Type error at generate_response: {e}")


def generate_target_errors_payload(
    target: str,
    error_detail: str,
    error_batch: List[Dict[str, Any]],
    error_count: int,
    success_count: int,
) -> Dict[str, Any]:
    try:
        return {
            "Target": target,
            "Error": error_detail,
            "Errors": error_batch,
            "ErrorCount": error_count,
            "SuccessCount": success_count,
        }
    except TypeError as e:
        logger.exception(f"Type error at generate_target_errors_payload: {e}")


def generate_batch_payload(
    target_path: str, success_count: int, errors: List[Dict[str, Any]], error_count: int
) -> Dict[str, Any]:
    try:
        return {
            "Target": target_path,
            "SuccessCount": success_count,
            "Errors": errors,
            "ErrorCount": error_count,
        }
    except TypeError as e:
        logger.exception(f"Type error at generate_batch_payload: {e}")


def process_targets(s3_target: Dict[str, str]) -> Dict[str, Any]:
    try:
        recipients_per_message = config.RECIPIENTS_PER_MESSAGE

        batch_errors, success_count = [], 0

        bucket_name, prefix, key, principal_id, timestamp = (
            s3_target["BucketName"],
            s3_target["Prefix"],
            s3_target["Key"],
            s3_target["PrincipalId"],
            s3_target["Timestamp"],
        )

        target_path = f"{bucket_name}/{prefix}/{key}"

        logger.info(f"getting {target_path}...")
        s3_object = get_s3_object(bucket_name=bucket_name, object_key=f"{prefix}/{key}")

        wrapper = io.TextIOWrapper(s3_object.get("Body"), encoding="utf-8")

        logger.info(f"grouping recipients by {recipients_per_message}...")

        batch_number = 1

        # group the recipients and send message to sqs
        for batch, failed_rows in batch_read_csv(wrapper, recipients_per_message):
            batch_id = f"{bucket_name}/{prefix}/{key}-{timestamp}-{batch_number}"
            batch_number += 1
            success_count += len(batch)

            try:
                if batch:
                    message = {
                        "BatchId": batch_id,
                        "Recipients": batch,
                        "Metadata": {
                            "UploadedBy": principal_id,
                            "Timestamp": timestamp,
                        },
                    }

                    logger.debug(f"sending batch {batch_number}...")

                    send_sqs_message(
                        queue_name=config.EMAIL_BATCH_QUEUE_NAME,
                        message_body=message,
                    )
            except Exception as e:
                logger.exception(
                    f"Failed to send sqs batch {batch_number} for {target_path}: {e}"
                )
                batch_errors.append(
                    {
                        "FailedRecipients": batch,
                        "Error": f"Failed to send batch: {str(e)}",
                    }
                )
        else:  # add to collection of failed rows when done
            logger.debug("adding failed rows to batch_errors!")
            batch_errors.extend(failed_rows)
    except Exception as e:
        logger.exception(f"Error at processing target: {e}")
        batch_errors.append({"Error": "Unexpected error", "Details": str(e)})
    finally:
        return generate_batch_payload(
            target_path,
            success_count,
            errors=batch_errors,
            error_count=len(batch_errors),
        )


def validate_template_fields(row: Dict[str, Any], ddb_table_name) -> List[str]:
    template_key = row.get("email_template")
    missing = []

    try:
        response: Dict[str, Any] = get_ddb_item(ddb_table_name, template_key)
    except Exception as e:
        logger.exception(f"Unexpected error with get_ddb_item: {e}")
        raise

    ddb_item: Dict[str, Dict[str, str]] = response.get("Item")

    # check ddb_item is valid, fields column is valid, and field is non-empty
    if ddb_item and ddb_item["fields"] and ddb_item["fields"]["S"]:
        fields = ddb_item.get("fields")["S"]
        for field in fields.split(","):
            field = field.strip()
            if field not in row or not row[field]:
                missing.append(field)

    return missing


def validate_basic_fields(row: Dict[str, Any], required_fields: List[str]) -> List[str]:
    missing = []

    for field in required_fields:
        field = field.strip()
        if field not in row or not row[field]:
            missing.append(field)

    return missing

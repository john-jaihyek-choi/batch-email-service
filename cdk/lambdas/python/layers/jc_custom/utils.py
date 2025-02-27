import json
import logging
import csv
import io
import os
import re
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, KeysView, TypedDict, Tuple
from aws_lambda_powertools.utilities.data_classes import S3Event, SQSEvent


logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class S3Target(TypedDict):
    BucketName: str
    Prefix: str
    Object: str
    PrincipalId: str
    Timestamp: str  # ISO format: "YYYYMMDD_HHMMSS"
    EventName: str


class SQSMessageTarget(TypedDict):
    BatchId: str
    Recipients: List[Dict[str, str]]
    UploadedBy: str
    Timestamp: str


def autofill_email_template(template: str, replacement_mapping: Dict[str, str]) -> str:
    try:
        pattern = (
            r"\{\{(" + "|".join(map(re.escape, replacement_mapping.keys())) + r")\}\}"
        )

        logger.debug(f"autofill pattern - {pattern}")

        # replace key-val pairs in replacement_mapping from the template
        template = re.sub(
            pattern,
            lambda match: replacement_mapping.get(match.group(1), match.group(0)),
            template,
        )

        logger.debug(f"updated template - {template}")

        return template
    except Exception as e:
        logger.exception(f"Error generating template: {e}")
        return "Unexpected error while autofilling email template"


def filter_s3_targets(
    s3_event: S3Event,
    allowed_buckets: Tuple[str, ...],
    allowed_prefix: Tuple[str, ...],
    allowed_suffix: Tuple[str, ...],
    allowed_s3_events: Tuple[str, ...],
) -> List[
    S3Target
]:  # retrieve all s3 targets in an S3Event and filter the relevant targets based on the allowed buckets, prefix, suffix, and s3 events.
    logger.debug("formatting and filtering s3 tagets...")

    res: List[S3Target] = []

    for record in s3_event["Records"]:
        try:
            event_type: str = record["eventName"]
            bucket_name: str = record["s3"]["bucket"]["name"]
            s3_object_key: str = record["s3"]["object"]["key"].split("/")
            principal_id: str = record["userIdentity"]["principalId"]

            prefix = "/".join(s3_object_key[:-1]) + "/"  # get s3 object prefix
            object = urllib.parse.unquote(s3_object_key[-1])  # get object (file) name

            if (
                "s3" in record
                and ("*" in allowed_buckets or bucket_name in allowed_buckets)
                and (
                    "*" in allowed_prefix
                    or any(prefix.startswith(allowed) for allowed in allowed_prefix)
                )
                and (
                    "*" in allowed_suffix
                    or any(object.endswith(allowed) for allowed in allowed_suffix)
                )
                and (
                    "*" in allowed_s3_events
                    or any(
                        event_type.startswith(allowed) for allowed in allowed_s3_events
                    )
                )
            ):
                res.append(
                    {
                        "BucketName": bucket_name,
                        "Prefix": prefix,
                        "Object": object,
                        "PrincipalId": principal_id,
                        "Timestamp": datetime.now(timezone.utc).strftime(
                            "%Y%m%d_%H%M%S"
                        ),
                        "EventName": event_type,
                    }
                )
        except Exception as e:
            logger.exception(f"Error filtering s3 target. Skipping record - {record}")

    return res


def filter_sqs_event(sqs_event: SQSEvent) -> List[SQSMessageTarget]:
    logger.info("formatting and filtering s3 tagets...")

    res: List[SQSMessageTarget] = []

    for record in sqs_event["Records"]:
        try:
            body = json.loads(record["body"])

            res.append(
                {
                    "MessageId": record["messageId"],
                    "ReceiptHandle": record["receiptHandle"],
                    "BatchName": body["BatchName"],
                    "BatchId": body["BatchId"],
                    "Recipients": body["Recipients"],
                    "UploadedBy": body["Metadata"]["UploadedBy"],
                    "Timestamp": body["Metadata"]["Timestamp"],
                }
            )
        except Exception as e:
            logger.exception(f"Error filtering sqs event. Skipping record - {record}")

    return res


def generate_csv(
    headers: KeysView[str], contents: List[Dict[str, Any]]
) -> str:  # Generate CSV content in memory
    logger.debug(
        f"generating csv in memory. {
        {
            "headers": headers,
            "contents": contents
        }
    }"
    )
    try:
        output = io.StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=headers)
        csv_writer.writeheader()

        for content in contents:
            csv_writer.writerow(content)

        csv_content = output.getvalue()
        output.close()

    except Exception as e:
        logger.error(f"Error generating csv: {e}")
    finally:
        return csv_content or ""


def generate_handler_response(
    status_code: int, message: Optional[str] = "", body: Optional[Any] = ""
):
    try:
        response = {
            "StatusCode": status_code,
            "Message": message,
            "Header": {"Content-Type": "application/json"},
            "Body": json.dumps(body),
        }
        return response
    except TypeError as e:
        logger.exception(f"Type error at generate_handler_response: {e}")

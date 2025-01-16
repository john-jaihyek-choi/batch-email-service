import json
import os
import logging
import urllib.parse
import boto3
import csv
import io
from datetime import datetime, timezone
from typing import Dict, Any, List
from http import HTTPStatus
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL"))
logger = logging.getLogger(__name__)

aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
recipients_per_message = os.getenv("RECIPIENTS_PER_MESSAGE", 50)
queue_name = os.getenv("EMAIL_BATCH_QUEUE_NAME")

sqs = boto3.client("sqs", aws_region)
s3 = boto3.client("s3", aws_region)


def lambda_handler(event: Dict[str, Any], context: Dict[Any, Any]):
    logger.debug("event: %s", json.dumps(event, indent=2))

    # handle invalid events
    if not event or "Records" not in event:
        logger.info("No s3 event records found")

        return {
            "StatusCode": HTTPStatus.BAD_REQUEST,
            "Body": {"Message": "Event missing - Valid S3 event is required"},
            "Headers": {"Content-Type": "application/json"},
        }

    try:
        # retrieve all target s3 objects and store it in target_objects array for further processing
        target_objects: List[Dict[str, str]] = format_and_filter_targets(event)

        if not target_objects:
            return {"StatusCode": 204, "Headers": {"Content-Type": "application/json"}}

        total_batches_sent, errors = 0, 0

        logger.info("successfully retrieved all targets from event")
        logger.debug("target_objects: %s", json.dumps(target_objects, indent=2))

        # open csv target and organize by N batch
        for target in target_objects:
            try:
                process_targets(target)
                total_batches_sent += 1
            except Exception as e:
                logger.error(f"Error processing target {target}: {e}")
                errors += 1

        logger.info("successfully processed the targets")
        return {
            "StatusCode": HTTPStatus.OK,
            "Body": {
                "Message": "Batch processing completed successfully",
                "processedRecords": len(target_objects),
                "errors": errors,
                "batchesSent": total_batches_sent,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "Headers": {"Content-Type": "application/json"},
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            "StatusCode": HTTPStatus.INTERNAL_SERVER_ERROR,
            "Body": {"Message": "An error occurred while processing the batch"},
            "Headers": {"Content-Type": "application/json"},
        }


def format_and_filter_targets(s3_event: Dict[str, Any]) -> List[Dict[str, str]]:
    logger.info("formatting and filtering tagets...")

    res = []
    # iterate on s3 event records and append to res
    for record in s3_event["Records"]:
        event_type: str = record["eventName"]
        bucket_name: str = record["s3"]["bucket"]["name"]
        s3_object_key: str = record["s3"]["object"]["key"].split("/")
        principal_id: str = record["userIdentity"]["principalId"]

        prefix = "/".join(s3_object_key[:-1])
        key = urllib.parse.unquote(s3_object_key[-1])

        if (
            "s3" in record
            and prefix == "batch/send"
            and key.endswith(".csv")
            and event_type.startswith("ObjectCreated")
        ):
            res.append(
                {
                    "bucket_name": bucket_name,
                    "prefix": prefix,
                    "key": key,
                    "principal_id": principal_id,
                }
            )

    return res


def process_targets(s3_target: Dict[str, str]) -> None:
    try:
        logger.info("processing targets... %s", s3_target)

        bucket_name, prefix, key, principal_id = (
            s3_target["bucket_name"],
            s3_target["prefix"],
            s3_target["key"],
            s3_target["principal_id"],
        )
        timestamp = datetime.now(timezone.utc)

        logger.info(f"getting {bucket_name}/{prefix}/{key}...")
        s3_object = s3.get_object(Bucket=bucket_name, Key=f"{prefix}/{key}")

        logger.info(f"getting {bucket_name}/{prefix}/{key}...")
        wrapper = io.TextIOWrapper(s3_object["Body"], encoding="utf-8")

        logger.info(f"grouping recipients by {recipients_per_message}...")

        # group the recipients and send message to sqs
        batch_number = 0
        for batch in batch_read_csv(wrapper, recipients_per_message):
            batch_number += 1
            send_sqs_message(
                bucket_name,
                prefix,
                key,
                timestamp,
                batch_number,
                s3_target["principal_id"],
                batch,
            )

    except Exception as e:
        logger.error(f"Unexpected error had occurred: {e}")


# Generator to read a CSV file in batches.


def batch_read_csv(file_obj, batch_size: int):
    batch = []
    csv_reader = csv.DictReader(file_obj)

    for row in csv_reader:
        if not row:  # Skip empty rows
            continue

        try:
            # validation logic goes here

            batch.append(row)
        except Exception as e:
            logger.error(f"Skipping malformed row: {row}. Error: {e}")
            continue

        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:  # Yield any remaining rows
        yield batch


def validate_row(row: Dict[str, Any]) -> bool:
    # Validates a single CSV row.

    required_fields = [
        "send_to",
        "first_name",
        "last_name",
        "send_from",
        "email_template",
    ]
    for field in required_fields:
        if field not in row or not row[field]:
            return False

    return True


def send_sqs_message(
    bucket_name: str,
    prefix: str,
    key: str,
    timestamp: datetime,
    batch_number: int,
    principal_id: str,
    recipient_batch: List[Dict[str, Any]],
) -> Dict[Any, Any]:
    try:
        aws_account_id = os.getenv("AWS_ACCOUNT_ID")

        logger.info("processing sqs message...")

        # organize message payload
        message = {
            "batchId": f"{bucket_name}/{prefix}/{key}-{str(timestamp)}-{batch_number}",
            "recipients": recipient_batch,
            "metadata": {"uploadedBy": principal_id, "timestamp": str(timestamp)},
        }

        logger.debug(f"sending batch {batch_number}...")

        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        # send message to sqs queue
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

        logger.debug(
            f"batch {batch_number} successfully sent: {
                     json.dumps(message)}"
        )

        return response

    except Exception as e:
        logger.error(f"Unexpected error had occurred: {e}")

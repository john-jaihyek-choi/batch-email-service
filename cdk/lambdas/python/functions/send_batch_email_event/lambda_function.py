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
recipients_per_message = int(os.getenv("RECIPIENTS_PER_MESSAGE", 50))
queue_name = os.getenv("EMAIL_BATCH_QUEUE_NAME")
csv_required_fields = os.getenv("CSV_REQUIRED_FIELDS")

sqs = boto3.client("sqs", aws_region)
s3 = boto3.client("s3", aws_region)


def lambda_handler(event: Dict[str, Any], context: Dict[Any, Any] = None):
    logger.info("event: %s", event)

    if not event or "Records" not in event:  # handle invalid events
        logger.info("No s3 event records found")

        return generate_response(
            status_code=HTTPStatus.BAD_REQUEST.value,
            message="Event missing - Valid S3 event is required",
        )

    try:
        target_objects: List[Dict[str, str]] = format_and_filter_targets(
            event
        )  # retrieve all target s3 objects and store it in target_objects array for further processing

        if not target_objects:
            return generate_response(
                status_code=HTTPStatus.NO_CONTENT.value,
                message="Target valid targets found",
            )

        target_errors, successful_recipients_count = [], 0

        logger.info("successfully retrieved all targets from event")
        logger.debug("target_objects: %s", json.dumps(target_objects, indent=2))

        for target in target_objects:  # open csv target and organize by N batch
            try:
                batch = process_targets(target)

                # add batch_errors to target_errors if any
                if batch.get("ErrorCount") > 0:
                    target_errors.append(
                        {
                            "Target": batch.get("Target"),
                            "Errors": batch.get("Errors"),
                            "ErrorCount": batch.get("ErrorCount"),
                        }
                    )
                successful_recipients_count += batch.get("SuccessCount", 0)

            except Exception as e:
                logger.error(f"Error processing target {target}: {e}")
                target_errors.append({"Target": target, "Error": str(e)})
                continue

        if not successful_recipients_count:  # handle failed batch case
            logger.info("Failed processing the batches")
            return generate_response(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
                message="Failed processing the batches",
                body={"FailedBatches": target_errors},
            )
        elif (
            successful_recipients_count and target_errors
        ):  # handle partial success case
            logger.info("partially processed the batches")
            return generate_response(
                status_code=HTTPStatus.PARTIAL_CONTENT.value,
                message="Batch partially processed",
                body={"FailedBatches": target_errors},
            )
        else:  # handle batch processing success
            logger.info("successfully processed the batches")

            return generate_response(
                status_code=HTTPStatus.OK.value,
                message="Batch processing completed successfully",
                body={},
            )

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return generate_response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
            message="An error occurred while processing the batch",
        )


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


def process_targets(s3_target: Dict[str, str]) -> Dict[str, Any]:
    batch_errors, success_count = [], 0

    try:
        logger.info("processing targets... %s", s3_target)

        bucket_name, prefix, key, principal_id, timestamp = (
            s3_target["BucketName"],
            s3_target["Prefix"],
            s3_target["Key"],
            s3_target["PrincipalId"],
            s3_target["Timestamp"],
        )

        s3_object = s3.get_object(Bucket=bucket_name, Key=f"{prefix}/{key}")
        target_path = f"{bucket_name}/{prefix}/{key}"

        logger.info(f"getting {target_path}...")
        wrapper = io.TextIOWrapper(s3_object["Body"], encoding="utf-8")

        logger.info(f"grouping recipients by {recipients_per_message}...")

        batch_number = 1

        # group the recipients and send message to sqs
        for batch, failed_rows in batch_read_csv(wrapper, recipients_per_message):
            batch_id = f"{bucket_name}/{prefix}/{key}-{timestamp}-{batch_number}"
            batch_number += 1
            success_count += len(batch)

            try:
                if batch:
                    send_sqs_message(
                        batch_id,
                        timestamp,
                        batch_number,
                        principal_id,
                        batch,
                    )

            except Exception as e:
                logger.error(f"Failed to send batch {batch_number}: {e}")
                batch_errors.append(
                    {
                        "FailedRecipients": batch,
                        "Error": f"Failed to send batch: {str(e)}",
                    }
                )
                continue

            if failed_rows:  # add to collection of failed rows to batch errors if any
                batch_errors.extend(failed_rows)

    except Exception as e:
        logger.error(f"Unexpected error had occurred: {e}")
        batch_errors.extend({"Error": str(e), "Target": s3_target})

    return {
        "Target": target_path,
        "SuccessCount": success_count,
        "Errors": batch_errors,
        "ErrorCount": len(batch_errors),
    }


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

            row_info = {**custom_fields, **row}

            if validate_result["IsValid"]:
                batch.append(row_info)
            else:
                row_errors.append(
                    {
                        "Error": "Missing required fields",
                        "RowNumber": row_number,
                        "MissingFields": validate_result["MissingFields"],
                    }
                )
        except Exception as e:
            row_errors.append({"Error": "Unidentified error", "RowNumber": row_number})
            continue

        if len(batch) == batch_size:
            yield batch, None
            batch = []

    if batch or row_errors:  # Yield batch or row_errors if any
        yield batch, row_errors


def validate_row(row: Dict[str, Any]) -> Dict[str, Any]:  # Validates a single CSV row.
    missing_fields = []

    for field in csv_required_fields.split(","):
        if field not in row or not row[field]:
            missing_fields.append(field)

    return {"IsValid": not missing_fields, "MissingFields": missing_fields}


def send_sqs_message(
    batch_id: str,
    timestamp: datetime,
    batch_number: int,
    principal_id: str,
    recipient_batch: List[Dict[str, Any]],
) -> Dict[Any, Any]:
    try:
        logger.info("processing sqs message...")

        # organize message payload
        message = {
            "BatchId": batch_id,
            "Recipients": recipient_batch,
            "Metadata": {"UploadedBy": principal_id, "Timestamp": timestamp},
        }

        logger.debug(f"sending batch {batch_number}...")

        # get queue url
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

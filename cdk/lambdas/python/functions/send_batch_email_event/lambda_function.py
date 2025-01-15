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

logging.basicConfig(
    level=os.getenv('LOG_LEVEL')
)
logger = logging.getLogger(__name__)

aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

sqs = boto3.client("sqs", aws_region)
s3 = boto3.client("s3", aws_region)

def lambda_handler(event: Dict[str, Any], context: Dict[Any, Any]):
    # handle invalid events
    if not event or "Records" not in event:
        return {
            "StatusCode": HTTPStatus.BAD_REQUEST,
            "Message": "Event missing - Valid S3 event is required."
        }

    # retrieve all target s3 objects and store it in target_objects array for further processing
    target_objects: List[Dict[str, str]] = format_and_filter_targets(event)

    logger.info(target_objects)

    # open csv target and organize by N batch
    for target in target_objects:
        process_targets(target)

    return {
        "ResponseMetadata": {
            "HTTPStatusCode": 200
        }
    }

def format_and_filter_targets(s3_event: Dict[str, Any]) -> List[Dict[str, str]]:
    res = []
    # iterate on s3 event records and append to res
    for record in s3_event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        s3_object_key = record["s3"]["object"]["key"].split("/")
        principal_id = record["userIdentity"]["principalId"]

        prefix = "/".join(s3_object_key[:-1])
        key = urllib.parse.unquote(s3_object_key[-1])

        if "s3" in record and prefix == "batch/send" and key.endswith(".csv"):
            res.append({
                "bucket_name": bucket_name,
                "prefix": prefix,
                "key": key,
                "principal_id": principal_id
            })

    return res

def process_targets(s3_target: Dict[str, str]) -> None:
    try:
        recipients_per_message = os.getenv("RECIPIENTS_PER_MESSAGE")

        bucket_name, prefix, key, principal_id = s3_target["bucket_name"], s3_target["prefix"], s3_target["key"], s3_target["principal_id"]
        timestamp = datetime.now(timezone.utc)

        s3_object = s3.get_object(Bucket=bucket_name, Key=f"{prefix}/{key}")
        wrapper = io.TextIOWrapper(s3_object["Body"], encoding="utf-8")

        recipient_batch, batch_number = [], 0
        for row in csv.DictReader(wrapper):
            recipient_batch.append(row)

            if len(recipient_batch) == recipients_per_message:
                batch_number += 1

                send_sqs_message(bucket_name, prefix, key, timestamp, batch_number, principal_id, recipient_batch)

                recipient_batch = []

        if recipient_batch:
            batch_number += 1
            send_sqs_message(bucket_name, prefix, key, timestamp, batch_number, principal_id, recipient_batch)

        

    except Exception as e:
        logger.error(f"Unexpected error had occurred: {e}")
        raise
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
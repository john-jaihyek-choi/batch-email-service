import json
import os
import logging
import boto3
from typing import Dict, Any, List
from http import HTTPStatus
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv('LOG_LEVEL')
)
logger = logging.getLogger(__name__)

sqs = boto3.client("sqs")
queue_url = os.getenv("TARGET_QUEUE_URL")

def lambda_handler(event, context):
    if not event or "Records" not in event:
        return {
            "StatusCode": HTTPStatus.BAD_REQUEST,
            "Message": "Event missing - Valid S3 event is required."
        }
    
    for record in event["Records"]:
        if "s3" in record:
            message_body = generate_message_payload(record)

            response = send_sqs_message(queue_url, message_body)

    return response

def send_sqs_message(queue_url: str, message_body: Dict[str, Any]):
    try:
        logger.info(queue_url)

        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )

        logger.info(response)

        return response

    except Exception as e:
        logger.error(f"Unexpected error had occurred: {e}")
        raise

def generate_message_payload(s3_record: Dict[str, Any]):
    
    bucket_name = s3_record["s3"]["bucket"]["name"]
    object_key = s3_record["s3"]["object"]["key"]

    message: Dict[str, Any] = {
        "bucket_name": bucket_name,
        "object_key": object_key
    }
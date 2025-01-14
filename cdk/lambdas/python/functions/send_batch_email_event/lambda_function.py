import json
import os
import logging
import boto3
from typing import Dict, Any
from http import HTTPStatus
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv('LOG_LEVEL')
)
logger = logging.getLogger(__name__)

sqs = boto3.client("sqs", os.getenv("AWS_DEFAULT_REGION", "us-east-2"))

def lambda_handler(event, context):
    # Example S3 Put Event
    # {
    #   "Records": [
    #     {
    #       "eventVersion": "2.0",
    #       "eventSource": "aws:s3",
    #       "awsRegion": "us-east-1",
    #       "eventTime": "1970-01-01T00:00:00.000Z",
    #       "eventName": "ObjectCreated:Put",
    #       "userIdentity": {
    #         "principalId": "EXAMPLE"
    #       },
    #       "requestParameters": {
    #         "sourceIPAddress": "127.0.0.1"
    #       },
    #       "responseElements": {
    #         "x-amz-request-id": "EXAMPLE123456789",
    #         "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
    #       },
    #       "s3": {
    #         "s3SchemaVersion": "1.0",
    #         "configurationId": "testConfigRule",
    #         "bucket": {
    #           "name": "example-bucket",
    #           "ownerIdentity": {
    #             "principalId": "EXAMPLE"
    #           },
    #           "arn": "arn:aws:s3:::example-bucket"
    #         },
    #         "object": {
    #           "key": "test%2Fkey",
    #           "size": 1024,
    #           "eTag": "0123456789abcdef0123456789abcdef",
    #           "sequencer": "0A1B2C3D4E5F678901"
    #         }
    #       }
    #     }
    #   ]
    # }

    queue_url = os.getenv("TARGET_QUEUE_URL")
    
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
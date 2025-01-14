from mypy_boto3_sqs.client import SQSClient, ListQueuesResultTypeDef
import pytest
from typing import Any, Generator
from moto import mock_aws
import boto3
import os
from http import HTTPStatus
from functions.send_batch_email_event.lambda_function import lambda_handler
from aws_lambda_powertools.utilities.data_classes import S3Event
import logging

logging.basicConfig(
    level=os.getenv('LOG_LEVEL')
)
logger = logging.getLogger(__name__)

# Test cases:
    # Valid event param
    # Invalid event param
        # empty s3 event
        # invalid eventName
            # Only accept ObjectCreated

@mock_aws
class TestSendBatchEmailEvent:
    def test_valid_event(self, create_mock_queue, valid_event: S3Event) -> None:
        response = lambda_handler(valid_event, {})

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "MessageId" in response
        assert response["MessageId"]

    def test_empty_event(self, create_mock_queue, empty_event: S3Event) -> None:
        response = lambda_handler(empty_event, {})

        assert response["StatusCode"] == HTTPStatus.BAD_REQUEST
        assert response["Message"] == "Event missing - Valid S3 event is required."

    def test_invalid_s3_event_name(self, create_mock_queue, invalid_event_name: S3Event) -> None:
        response = lambda_handler(invalid_event_name, {})

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "MessageId" in response
        assert response["MessageId"]


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_DEFAULT_REGION"] = "us-east-2"

@pytest.fixture(scope="module")
def create_mock_queue(aws_credentials):
    with mock_aws():
        sqs: SQSClient = boto3.client("sqs", os.getenv("AWS_DEFAULT_REGION"))
        queue = sqs.create_queue(QueueName="test-queue1")
        os.environ["TARGET_QUEUE_URL"] = queue["QueueUrl"]
        yield

@pytest.fixture(scope="class")
def valid_event() -> S3Event:
    return {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "EXAMPLE"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "batchEmailServiceBucket",
                        "ownerIdentity": {
                                "principalId": "EXAMPLE"
                        },
                        "arn": "arn:aws:s3:::example-bucket"
                    },
                    "object": {
                        "key": "batch/send/example.csv",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901"
                    }
                }
            }
        ]
    }

@pytest.fixture(scope="class")
def empty_event() -> S3Event:
    return {}

@pytest.fixture(scope="class")
def invalid_event_name() -> S3Event:
    return {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectRemoved",
                "userIdentity": {
                    "principalId": "EXAMPLE"
                },
                "requestParameters": {
                    "sourceIPAddress": "127.0.0.1"
                },
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": "batchEmailServiceBucket",
                        "ownerIdentity": {
                                "principalId": "EXAMPLE"
                        },
                        "arn": "arn:aws:s3:::example-bucket"
                    },
                    "object": {
                        "key": "batch/send/example.csv",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901"
                    }
                }
            }
        ]
    }
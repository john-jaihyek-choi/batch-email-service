from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_s3.client import S3Client
import pytest
from typing import List
from moto import mock_aws
import boto3
import os
import logging
import json
from http import HTTPStatus
from functions.send_batch_email_event.lambda_function import lambda_handler
from aws_lambda_powertools.utilities.data_classes import S3Event

logging.basicConfig(level=os.getenv("LOG_LEVEL"))
logger = logging.getLogger(__name__)


# Test Cases
@mock_aws
@pytest.mark.parametrize(
    "event_fixture, expected_message",
    [
        ("valid_single_record_event", "Batch processing completed successfully"),
        ("valid_multi_record_event", "Batch processing completed successfully"),
    ],
)
def test_valid_events(request, event_fixture, expected_message):
    event = request.getfixturevalue(event_fixture)
    response = lambda_handler(event, {})

    assert response["StatusCode"] == HTTPStatus.OK
    assert response["Message"] == expected_message


def test_missing_required_csv_fields(
    missing_required_csv_field_event: S3Event,
) -> None:
    response = lambda_handler(missing_required_csv_field_event, {})

    assert response["StatusCode"] == HTTPStatus.PARTIAL_CONTENT
    assert response["Message"] == "Batch partially processed"
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_empty_event(empty_event: S3Event) -> None:
    response = lambda_handler(empty_event, {})

    assert response["StatusCode"] == HTTPStatus.BAD_REQUEST
    assert response["Message"] == "Event missing - Valid S3 event is required"


def test_invalid_s3_event_name(invalid_event_name: S3Event) -> None:
    response = lambda_handler(invalid_event_name, {})

    assert response["StatusCode"] == HTTPStatus.NO_CONTENT
    assert response["Message"] == "Target valid targets found"


def test_sent_message_validation(
    create_mock_queue,
):
    sqs: SQSClient = create_mock_queue
    queue = sqs.get_queue_url(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME"))
    message = sqs.receive_message(QueueUrl=queue["QueueUrl"])

    assert "Recipients" in json.loads(message["Messages"][0]["Body"])


# Fixtures
@pytest.fixture(scope="module", autouse=True)
def aws_credential_overwrite():
    os.environ["AWS_ACCOUNT_ID"] = "123456789012"


@pytest.fixture(scope="module", autouse=True)
def create_mock_queue():
    with mock_aws():
        sqs: SQSClient = boto3.client("sqs", os.getenv("AWS_DEFAULT_REGION"))
        sqs.create_queue(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME"))
        yield sqs


@pytest.fixture(scope="module", autouse=True)
def create_mock_s3(aws_credential_overwrite):
    aws_region = os.getenv("AWS_DEFAULT_REGION")
    bucket_name = os.getenv("EMAIL_SERVICE_BUCKET_NAME")

    with mock_aws():
        s3: S3Client = boto3.client("s3", aws_region)
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": aws_region,
            },
        )

        mock_recipients_data_path: List[str] = [
            "../../assets/batch/example-recipients-list-1.csv",
            "../../assets/batch/example-recipients-list-2.csv",
            "../../assets/batch/bad-example-missing-required-column.csv",
        ]

        for path in mock_recipients_data_path:
            file_name = path.split("/")[-1]

            with open(path) as file:
                recipients_list = file.read()

            s3.put_object(
                Bucket=bucket_name,
                Key=f"batch/send/{file_name}",
                Body=recipients_list,
            )

        yield s3


@pytest.fixture
def valid_single_record_event() -> S3Event:
    file_name = "example-recipients-list-1.csv"

    return {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": os.getenv("AWS_DEFAULT_REGION"),
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "EXAMPLE"},
                "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": os.getenv("EMAIL_SERVICE_BUCKET_NAME"),
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": f"arn:aws:s3:::{os.getenv("EMAIL_SERVICE_BUCKET_NAME")}",
                    },
                    "object": {
                        "key": f"batch/send/{file_name}",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901",
                    },
                },
            }
        ]
    }


@pytest.fixture
def valid_multi_record_event() -> S3Event:
    file_names = ["example-recipients-list-1.csv", "example-recipients-list-2.csv"]

    payload = {"Records": []}

    for name in file_names:

        record = {
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": os.getenv("AWS_DEFAULT_REGION"),
            "eventTime": "1970-01-01T00:00:00.000Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "EXAMPLE"},
            "requestParameters": {"sourceIPAddress": "127.0.0.1"},
            "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "testConfigRule",
                "bucket": {
                    "name": os.getenv("EMAIL_SERVICE_BUCKET_NAME"),
                    "ownerIdentity": {"principalId": "EXAMPLE"},
                    "arn": f"arn:aws:s3:::{os.getenv("EMAIL_SERVICE_BUCKET_NAME")}",
                },
                "object": {
                    "key": f"batch/send/{name}",
                    "size": 1024,
                    "eTag": "0123456789abcdef0123456789abcdef",
                    "sequencer": "0A1B2C3D4E5F678901",
                },
            },
        }

        payload["Records"].append(record)

    return payload


@pytest.fixture
def missing_required_csv_field_event() -> S3Event:
    file_name = "bad-example-missing-required-column.csv"

    return {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": os.getenv("AWS_DEFAULT_REGION"),
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "EXAMPLE"},
                "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": os.getenv("EMAIL_SERVICE_BUCKET_NAME"),
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": f"arn:aws:s3:::{os.getenv("EMAIL_SERVICE_BUCKET_NAME")}",
                    },
                    "object": {
                        "key": f"batch/send/{file_name}",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901",
                    },
                },
            }
        ]
    }


@pytest.fixture
def empty_event() -> S3Event:
    return None


@pytest.fixture
def invalid_event_name() -> S3Event:
    file_name = "example-recipients-list-1.csv"

    return {
        "Records": [
            {
                "eventVersion": "2.0",
                "eventSource": "aws:s3",
                "awsRegion": os.getenv("AWS_DEFAULT_REGION"),
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectRemoved",
                "userIdentity": {"principalId": "EXAMPLE"},
                "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                "responseElements": {
                    "x-amz-request-id": "EXAMPLE123456789",
                    "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "testConfigRule",
                    "bucket": {
                        "name": os.getenv("EMAIL_SERVICE_BUCKET_NAME"),
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": f"arn:aws:s3:::{os.getenv("EMAIL_SERVICE_BUCKET_NAME")}",
                    },
                    "object": {
                        "key": f"batch/send/{file_name}",
                        "size": 1024,
                        "eTag": "0123456789abcdef0123456789abcdef",
                        "sequencer": "0A1B2C3D4E5F678901",
                    },
                },
            }
        ]
    }

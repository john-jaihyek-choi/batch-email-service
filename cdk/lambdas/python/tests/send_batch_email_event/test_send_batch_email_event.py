# stdlib
import pytest
import os
import logging
import json
from typing import List, Dict, Any
from http import HTTPStatus

# external libararies
from pytest import FixtureRequest
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_s3.client import S3Client
from dotenv import load_dotenv

# local modules
from send_batch_email_event.main import lambda_handler

load_dotenv()


logger = logging.getLogger(__name__)


# Test Cases
@pytest.mark.parametrize(
    "valid_events, expected_message",
    [
        ("valid_single_record_event", "Batch processing completed successfully"),
        ("valid_multi_record_event", "Batch processing completed successfully"),
    ],
)
def test_valid_events(request: FixtureRequest, valid_events, expected_message):
    event = request.getfixturevalue(valid_events)
    response = lambda_handler(event, {})

    assert response["StatusCode"] == HTTPStatus.OK
    assert response["Message"] == expected_message


def test_partial_success(partial_success_event):
    response = lambda_handler(partial_success_event, {})

    assert response["StatusCode"] == HTTPStatus.PARTIAL_CONTENT
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_missing_required_csv_fields(
    mocked_s3: S3Client,
    missing_basic_required_csv_field_event: Dict[str, Any],
) -> None:
    response = lambda_handler(missing_basic_required_csv_field_event, None)

    body = json.loads(response.get("Body", ""))
    failed_batches = body.get("FailedBatches", [])

    object_relocation_successful = failed_s3_object_moved_successfully(
        s3=mocked_s3, s3_batches=failed_batches
    )

    assert object_relocation_successful
    assert response["StatusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["Message"] == "Failed processing the batches"
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_missing_template_specific_csv_fields(
    mocked_s3: S3Client,
    missing_template_specific_field_event: Dict[str, Any],
) -> None:
    response = lambda_handler(missing_template_specific_field_event, {})

    body = json.loads(response["Body"])
    failed_batches = body.get("FailedBatches", [])

    object_relocation_successful = failed_s3_object_moved_successfully(
        s3=mocked_s3, s3_batches=failed_batches
    )

    assert object_relocation_successful
    assert response["StatusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["Message"] == "Failed processing the batches"
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_empty_event(empty_event: Dict[str, Any]) -> None:
    response = lambda_handler(empty_event, {})

    assert response["StatusCode"] == HTTPStatus.BAD_REQUEST
    assert response["Message"] == "Invalid event: Missing 'Records' key"


def test_empty_s3_content(empty_s3_content_event: Dict[str, Any]) -> None:
    response = lambda_handler(empty_s3_content_event, {})

    assert response["StatusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["Message"] == "Failed processing the batches"


def test_invalid_s3_event_name(invalid_event_name: Dict[str, Any]) -> None:
    response = lambda_handler(invalid_event_name, {})

    assert response["StatusCode"] == HTTPStatus.NO_CONTENT
    assert response["Message"] == "No valid targets found"


def test_sent_message_validation(
    mocked_sqs: SQSClient,
):
    sqs = mocked_sqs
    queue = sqs.get_queue_url(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME", ""))
    message = sqs.receive_message(QueueUrl=queue["QueueUrl"])

    assert "Recipients" in json.loads(message["Messages"][0]["Body"])


@pytest.fixture
def valid_single_record_event() -> Dict[str, Any]:
    file_name = "valid-recipients-list-1.csv"

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
                        "name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": f"arn:aws:s3:::{os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")}",
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
def valid_multi_record_event() -> Dict[str, Any]:
    file_names = ["valid-recipients-list-1.csv", "valid-recipients-list-2.csv"]

    payload: Dict[str, Any] = {"Records": []}

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
                    "name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
                    "ownerIdentity": {"principalId": "EXAMPLE"},
                    "arn": f"arn:aws:s3:::{os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")}",
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
def partial_success_event() -> Dict[str, Any]:
    # file_names = ["partially-complete-list.csv", "partially-complete-list-1.csv"]
    file_names = ["partially-complete-list.csv"]

    payload: Dict[str, Any] = {"Records": []}

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
                    "name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
                    "ownerIdentity": {"principalId": "EXAMPLE"},
                    "arn": f"arn:aws:s3:::{os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")}",
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
def missing_basic_required_csv_field_event() -> Dict[str, Any]:
    file_name = "missing-basic-required-column.csv"

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
                        "name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": f"arn:aws:s3:::{os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")}",
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
def missing_template_specific_field_event() -> Dict[str, Any]:
    file_name = "missing-template-specific-column.csv"

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
                        "name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": f"arn:aws:s3:::{os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")}",
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
def empty_event() -> None:
    return None


@pytest.fixture
def empty_s3_content_event() -> Dict[str, Any]:
    file_name = "empty-s3-content.csv"

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
                        "name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": f"arn:aws:s3:::{os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")}",
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
def invalid_event_name() -> Dict[str, Any]:
    file_name = "valid-recipients-list-1.csv"

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
                        "name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
                        "ownerIdentity": {"principalId": "EXAMPLE"},
                        "arn": f"arn:aws:s3:::{os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")}",
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


def failed_s3_object_moved_successfully(
    s3: S3Client, s3_batches: List[Dict[str, Any]]
) -> bool:
    for batch in s3_batches:
        target_path = batch.get("Target", "").split("/")
        bucket, key = target_path[0], "/".join(target_path[1:])
        object = target_path[-1]

        # check source object
        try:
            s3.get_object(Bucket=bucket, Key=key)
            logger.info(f"Source object not deleted successfully - {bucket}/{key}")
            return False
        except s3.exceptions.NoSuchKey as e:
            logger.info(f"No key found in original source - {bucket}/{key}: {e}")

        # check destination object
        try:
            new_destination_key = (
                f"{os.getenv("BATCH_INITIATION_ERROR_S3_PREFIX")}{object}"
            )
            s3.get_object(Bucket=bucket, Key=new_destination_key)
            logger.info(
                f"New key found in destination - {bucket}/{new_destination_key}"
            )
        except s3.exceptions.NoSuchKey as e:
            logger.exception(
                f"No such key found in new destination - {bucket}/{new_destination_key}: {e}"
            )
            return False

    return True

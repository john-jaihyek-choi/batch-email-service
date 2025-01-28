import boto3.exceptions
import pytest
import os
import logging
import json
import boto3
from dotenv import load_dotenv
from typing import List, Dict, Any
from moto import mock_aws
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_s3.client import S3Client
from mypy_boto3_ses.client import SESClient
from http import HTTPStatus
from functions.send_batch_email_event.lambda_function import lambda_handler
from aws_lambda_powertools.utilities.data_classes import S3Event

load_dotenv()

# Restrict external library logs to WARNING due to noise
hide_logs = ["boto3_helper", "boto3", "urlib3", "botocore"]
for module in hide_logs:
    logging.getLogger(module).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


# Test Cases
@mock_aws
@pytest.mark.parametrize(
    "valid_events, expected_message",
    [
        ("valid_single_record_event", "Batch processing completed successfully"),
        ("valid_multi_record_event", "Batch processing completed successfully"),
    ],
)
def test_valid_events(request, valid_events, expected_message):
    event = request.getfixturevalue(valid_events)
    response = lambda_handler(event, {})

    assert response["StatusCode"] == HTTPStatus.OK
    assert response["Message"] == expected_message


def test_partial_success(partial_success_event):
    response = lambda_handler(partial_success_event, {})

    assert response["StatusCode"] == HTTPStatus.PARTIAL_CONTENT
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_missing_required_csv_fields(
    create_mock_s3: S3Client,
    missing_required_csv_field_event: S3Event,
) -> None:
    response = lambda_handler(missing_required_csv_field_event, {})

    body = json.loads(response["Body"])
    failed_batches = body.get("FailedBatches", [])

    object_relocation_successful = failed_s3_object_moved_successfully(
        s3=create_mock_s3, s3_batches=failed_batches
    )

    assert object_relocation_successful
    assert response["StatusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["Message"] == "Failed processing the batches"
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_empty_event(empty_event: S3Event) -> None:
    response = lambda_handler(empty_event, {})

    assert response["StatusCode"] == HTTPStatus.BAD_REQUEST
    assert response["Message"] == "Invalid event: Missing 'Records' key"


def test_empty_s3_content(empty_s3_content_event: S3Event) -> None:
    response = lambda_handler(empty_s3_content_event, {})

    assert response["StatusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["Message"] == "Failed processing the batches"


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
    logger.info("Overwriting environment variables for testing...")

    os.environ["BATCH_EMAIL_SERVICE_BUCKET_NAME"] = "test-mock-s3-bucket"


@pytest.fixture(scope="module", autouse=True)
def create_mock_queue():
    with mock_aws():
        sqs: SQSClient = boto3.client("sqs", os.getenv("AWS_DEFAULT_REGION"))
        sqs.create_queue(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME"))
        yield sqs


@pytest.fixture(scope="module", autouse=True)
def create_mock_s3():
    aws_region = os.getenv("AWS_DEFAULT_REGION")
    bucket_name = os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")

    with mock_aws():
        s3: S3Client = boto3.client("s3", aws_region)
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": aws_region,
            },
        )

        mock_recipients_data_path: List[str] = [
            "../../assets/batch/examples/valid-recipients-list-1.csv",
            "../../assets/batch/examples/valid-recipients-list-2.csv",
            "../../assets/batch/examples/partially-complete-list.csv",
            "../../assets/batch/examples/partially-complete-list-1.csv",
            "../../assets/batch/examples/missing-required-column.csv",
            "../../assets/batch/examples/empty-s3-content.csv",
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

        mock_email_templates_path: List[str] = [
            "../../assets/templates/send-batch-failure-email-template.html",
            "../../assets/templates/send-batch-failure-email-template.txt",
        ]

        for path in mock_email_templates_path:
            file_name = path.split("/")[-1]

            with open(path) as file:
                template = file.read()

            s3.put_object(
                Bucket=bucket_name,
                Key=f"templates/{file_name}",
                Body=template,
            )

        yield s3


@pytest.fixture(scope="module", autouse=True)
def setup_ses():
    # Initialize SES client
    ses_client: SESClient = boto3.client(
        "ses", region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    ses_client.verify_email_identity(EmailAddress=os.getenv("SES_NO_REPLY_SENDER"))


@pytest.fixture
def valid_single_record_event() -> S3Event:
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
def valid_multi_record_event() -> S3Event:
    file_names = ["valid-recipients-list-1.csv", "valid-recipients-list-2.csv"]

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
def partial_success_event() -> S3Event:
    file_names = ["partially-complete-list.csv", "partially-complete-list-1.csv"]

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
def missing_required_csv_field_event() -> S3Event:
    file_name = "missing-required-column.csv"

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
def empty_event() -> S3Event:
    return None


@pytest.fixture
def empty_s3_content_event() -> S3Event:
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
def invalid_event_name() -> S3Event:
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
                f"{os.getenv("BATCH_INITIATION_ERROR_S3_PREFIX")}/{object}"
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

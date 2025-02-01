import boto3.exceptions
import pytest
import os
import logging
import json
import boto3
from pytest import FixtureRequest
from typing import Callable, List, Dict, Any
from http import HTTPStatus
from moto import mock_aws
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_s3.client import S3Client
from mypy_boto3_ses.client import SESClient
from mypy_boto3_dynamodb.client import DynamoDBClient
from aws_lambda_powertools.utilities.data_classes import S3Event
from dotenv import load_dotenv

load_dotenv()

# # importing after loading environment due to dependencies
# from functions.send_batch_email_event.lambda_function import lambda_handler


# Restrict external library logs to WARNING due to noise
hide_logs = ["boto3_helper", "boto3", "urlib3", "botocore"]
for module in hide_logs:
    logging.getLogger(module).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

HandlerFunction = Callable[[Dict[str, Any], Any], Dict[str, Any]]


# Fixtures
@pytest.fixture(scope="module", autouse=True)
def aws_credential_overwrite():
    logger.info("Overwriting environment variables for testing...")

    os.environ["BATCH_EMAIL_SERVICE_BUCKET_NAME"] = "test-mock-s3-bucket"
    os.environ["TEST_EXAMPLE_BATCH_PATH"] = (
        "/Users/jchoi950/Dev/web/batch-email-service/cdk/assets/batch/examples"
    )
    os.environ["TEST_EXAMPLE_TEMPLATE_PATH"] = (
        "/Users/jchoi950/Dev/web/batch-email-service/cdk/assets/templates"
    )
    os.environ["TEST_EXAMPLE_DB_PATH"] = (
        "/Users/jchoi950/Dev/web/batch-email-service/cdk/assets/db/example/example-db.json"
    )
    os.environ["TEMPLATE_METADATA_TABLE_NAME"] = "mock-ddb-table"


@mock_aws
@pytest.fixture(scope="module", autouse=True)
def mocked_sqs():
    with mock_aws():
        try:
            sqs: SQSClient = boto3.client("sqs", os.getenv("AWS_DEFAULT_REGION"))
            sqs.create_queue(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME"))
        except Exception as e:
            pytest.fail(f"Failed setting up mock sqs {e}")
        yield sqs


@mock_aws
@pytest.fixture(scope="module", autouse=True)
def mocked_s3():
    aws_region: str = os.getenv("AWS_DEFAULT_REGION")
    bucket_name: str = os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")

    with mock_aws():
        try:
            s3: S3Client = boto3.client("s3", aws_region)
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": aws_region,
                },
            )

            test_assets = [
                {
                    "local_path": os.getenv("TEST_EXAMPLE_BATCH_PATH"),
                    "s3_prefix": "batch/send/",
                },
                {
                    "local_path": os.getenv("TEST_EXAMPLE_TEMPLATE_PATH"),
                    "s3_prefix": "templates/",
                },
            ]

            for asset in test_assets:
                upload_directory_to_mocked_s3(
                    s3=s3,
                    bucket_name=bucket_name,
                    local_path=asset["local_path"],
                    s3_prefix=asset["s3_prefix"],
                )

        except Exception as e:
            pytest.fail(f"Failed setting up mock s3: {e}")

        yield s3


@pytest.fixture(scope="module", autouse=True)
def mocked_ses():
    # Initialize SES client
    with mock_aws():
        try:
            ses: SESClient = boto3.client(
                "ses", region_name=os.getenv("AWS_DEFAULT_REGION")
            )
            ses.verify_email_identity(EmailAddress=os.getenv("SES_NO_REPLY_SENDER"))
        except Exception as e:
            pytest.fail(f"Failed to setup ses client and/or verify email identity: {e}")
        yield ses


@pytest.fixture(scope="module", autouse=True)
def mocked_ddb():
    # Create table and put items
    db_path: str = os.getenv("TEST_EXAMPLE_DB_PATH")

    with mock_aws():
        try:
            table_name = os.getenv("TEMPLATE_METADATA_TABLE_NAME")

            ddb: DynamoDBClient = boto3.client(
                "dynamodb", region_name=os.getenv("AWS_DEFAULT_REGION")
            )

            ddb.create_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {"AttributeName": "template_key", "AttributeType": "S"},
                ],
                KeySchema=[{"AttributeName": "template_key", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )

            ddb.get_waiter("table_exists").wait(TableName=table_name)

            batch_write_item = []

            with open(db_path) as file:
                rows = json.load(file)

                for row in rows:
                    batch_write_item.append({"PutRequest": {"Item": row}})

            ddb.batch_write_item(
                RequestItems={table_name: batch_write_item},
                ReturnConsumedCapacity="TOTAL",
            )

        except Exception as e:
            pytest.fail(f"Failed to setup mock ddb: {e}")
        yield ddb


@pytest.fixture
def handler() -> HandlerFunction:
    from functions.send_batch_email_event.lambda_function import lambda_handler

    return lambda_handler


# Test Cases
@pytest.mark.parametrize(
    "valid_events, expected_message",
    [
        ("valid_single_record_event", "Batch processing completed successfully"),
        ("valid_multi_record_event", "Batch processing completed successfully"),
    ],
)
def test_valid_events(
    request: FixtureRequest, handler: HandlerFunction, valid_events, expected_message
):
    event = request.getfixturevalue(valid_events)
    response = handler(event, {})

    assert response["StatusCode"] == HTTPStatus.OK
    assert response["Message"] == expected_message


def test_partial_success(handler: HandlerFunction, partial_success_event):
    response = handler(partial_success_event, {})

    assert response["StatusCode"] == HTTPStatus.PARTIAL_CONTENT
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_missing_required_csv_fields(
    handler: HandlerFunction,
    mocked_s3: S3Client,
    missing_basic_required_csv_field_event: S3Event,
) -> None:
    response = handler(missing_basic_required_csv_field_event, {})

    body = json.loads(response["Body"])
    failed_batches = body.get("FailedBatches", [])

    object_relocation_successful = failed_s3_object_moved_successfully(
        s3=mocked_s3, s3_batches=failed_batches
    )

    assert object_relocation_successful
    assert response["StatusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["Message"] == "Failed processing the batches"
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_missing_template_specific_csv_fields(
    handler: HandlerFunction,
    mocked_s3: S3Client,
    missing_template_specific_field_event: S3Event,
) -> None:
    response = handler(missing_template_specific_field_event, {})

    body = json.loads(response["Body"])
    failed_batches = body.get("FailedBatches", [])

    object_relocation_successful = failed_s3_object_moved_successfully(
        s3=mocked_s3, s3_batches=failed_batches
    )

    assert object_relocation_successful
    assert response["StatusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["Message"] == "Failed processing the batches"
    assert len(json.loads(response["Body"])["FailedBatches"][0]) > 0


def test_empty_event(handler: HandlerFunction, empty_event: S3Event) -> None:
    response = handler(empty_event, {})

    assert response["StatusCode"] == HTTPStatus.BAD_REQUEST
    assert response["Message"] == "Invalid event: Missing 'Records' key"


def test_empty_s3_content(
    handler: HandlerFunction, empty_s3_content_event: S3Event
) -> None:
    response = handler(empty_s3_content_event, {})

    assert response["StatusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["Message"] == "Failed processing the batches"


def test_invalid_s3_event_name(
    handler: HandlerFunction, invalid_event_name: S3Event
) -> None:
    response = handler(invalid_event_name, {})

    assert response["StatusCode"] == HTTPStatus.NO_CONTENT
    assert response["Message"] == "Target valid targets found"


def test_sent_message_validation(
    mocked_sqs: SQSClient,
):
    sqs = mocked_sqs
    queue = sqs.get_queue_url(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME"))
    message = sqs.receive_message(QueueUrl=queue["QueueUrl"])

    assert "Recipients" in json.loads(message["Messages"][0]["Body"])


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
    # file_names = ["partially-complete-list.csv", "partially-complete-list-1.csv"]
    file_names = ["partially-complete-list.csv"]

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
def missing_basic_required_csv_field_event() -> S3Event:
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
def missing_template_specific_field_event() -> S3Event:
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


def upload_directory_to_mocked_s3(
    s3: S3Client, bucket_name: str, local_path: str, s3_prefix: str = ""
):
    # Walk through the local directory
    for root, _, files in os.walk(local_path):
        for file in files:
            local_file_path = os.path.join(root, file)

            # Get the relative path for S3 key
            relative_path = os.path.relpath(local_file_path, local_path)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")

            # Upload file to mock S3
            s3.upload_file(local_file_path, bucket_name, s3_key)

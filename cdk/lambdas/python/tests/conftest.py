# stdlib
import pytest
import os
import logging
import json
from typing import Generator, cast, List, Dict, Any, TypedDict

# external libararies
import boto3.exceptions
import boto3
from moto import mock_aws
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_s3.client import S3Client
from mypy_boto3_ses.client import SESClient
from mypy_boto3_sesv2.client import SESV2Client
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_s3.literals import BucketLocationConstraintType
from mypy_boto3_dynamodb.type_defs import WriteRequestUnionTypeDef
from dotenv import load_dotenv

# local modules
from tests.types import S3EventRecordPayload
from jc_custom.boto3_helper import aws_client

load_dotenv()

logger = logging.getLogger(__name__)

aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")


@pytest.fixture(scope="session", autouse=True)
def logger_setup():
    # Restrict external library logs to WARNING due to noise
    hide_logs = ["boto3_helper", "boto3", "urlib3", "botocore", "s3transfer"]
    for module in hide_logs:
        logging.getLogger(module).setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


@pytest.fixture(scope="module", autouse=True)
def mocked_aws():
    with mock_aws():
        logger.warning("Starting mock_aws session...")
        # Any pre-load configurations if needed
        yield


@pytest.fixture(scope="module", autouse=True)
def mocked_sqs(mocked_aws) -> Generator[SQSClient, None, None]:
    try:
        sqs: SQSClient = aws_client.get_client("sqs", aws_region)
        sqs.create_queue(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME", ""))

    except Exception as e:
        pytest.fail(f"Failed setting up mock sqs {e}")
    yield sqs


@pytest.fixture(scope="module", autouse=True)
def mocked_s3(mocked_aws) -> Generator[S3Client, None, None]:
    aws_region = cast(
        BucketLocationConstraintType, os.getenv("AWS_DEFAULT_REGION", "us-east-2")
    )
    bucket_name: str = os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME", "")

    try:
        s3: S3Client = aws_client.get_client("s3", aws_region)
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": aws_region,
            },
        )

        test_assets = [
            {
                "local_path": os.getenv("TEST_EXAMPLE_BATCH_PATH", ""),
                "s3_prefix": "batch/send/",
            },
            {
                "local_path": os.getenv("TEST_EXAMPLE_TEMPLATE_PATH", ""),
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
def mocked_ses(mocked_aws) -> Generator[SESV2Client, None, None]:
    try:
        sesv2: SESV2Client = aws_client.get_client("sesv2", aws_region)
        ses: SESClient = aws_client.get_client("ses", aws_region)
        ses.verify_email_identity(EmailAddress=os.getenv("SES_NO_REPLY_SENDER", ""))
    except Exception as e:
        pytest.fail(f"Failed to setup ses client and/or verify email identity: {e}")
    yield sesv2


@pytest.fixture(scope="module", autouse=True)
def mocked_ddb(mocked_aws) -> Generator[DynamoDBClient, None, None]:
    db_path: str = os.getenv("TEST_EXAMPLE_DB_PATH", "")
    try:
        table_name = os.getenv("TEMPLATE_METADATA_TABLE_NAME", "")
        ddb: DynamoDBClient = aws_client.get_client("dynamodb", aws_region)

        ddb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "template_key", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "template_key", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )

        ddb.get_waiter("table_exists").wait(TableName=table_name)

        batch_write_item: List[WriteRequestUnionTypeDef] = []

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


@pytest.fixture(scope="module")
def generate_mock_s3_lambda_event():
    def generate_records(records: List[S3EventRecordPayload]) -> Dict[str, Any]:
        res = []

        for record in records:
            res.append(
                {
                    "eventVersion": "2.0",
                    "eventSource": "aws:s3",
                    "awsRegion": record["bucket_region"],
                    "eventTime": "1970-01-01T00:00:00.000Z",
                    "eventName": record["event_name"],
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
                            "name": record["bucket_name"],
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                            "arn": f"arn:aws:s3:::{record["bucket_name"]}",
                        },
                        "object": {
                            "key": record["object_key"],
                            "size": 1024,
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                        },
                    },
                }
            )

        return {"Records": res}

    return generate_records


def upload_directory_to_mocked_s3(
    s3: S3Client, bucket_name: str, local_path: str, s3_prefix: str = ""
):
    # Walk through the local directory
    for root, _, files in os.walk(local_path):
        for file in files:
            local_file_path = os.path.join(root, file)

            relative_path = os.path.relpath(local_file_path, local_path)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")

            s3.upload_file(local_file_path, bucket_name, s3_key)

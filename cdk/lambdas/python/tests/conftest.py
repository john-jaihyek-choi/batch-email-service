# stdlib
import pytest
import os
import logging
import json
from typing import Generator, cast, List

# external libararies
import boto3.exceptions
import boto3
from moto import mock_aws
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_s3.client import S3Client
from mypy_boto3_ses.client import SESClient
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_s3.literals import BucketLocationConstraintType
from mypy_boto3_dynamodb.type_defs import WriteRequestUnionTypeDef
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def logger_setup():
    # Restrict external library logs to WARNING due to noise
    hide_logs = os.getenv("HIDE_LOGS_LIST", "").split(",")
    for module in hide_logs:
        logging.getLogger(module).setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


@pytest.fixture(scope="module", autouse=True)
def mocked_sqs() -> Generator[SQSClient, None, None]:
    with mock_aws():
        try:
            sqs: SQSClient = boto3.client("sqs", os.getenv("AWS_DEFAULT_REGION"))
            sqs.create_queue(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME", ""))
        except Exception as e:
            pytest.fail(f"Failed setting up mock sqs {e}")
        yield sqs


@pytest.fixture(scope="module", autouse=True)
def mocked_s3() -> Generator[S3Client, None, None]:
    aws_region = cast(
        BucketLocationConstraintType, os.getenv("AWS_DEFAULT_REGION", "us-east-2")
    )
    bucket_name: str = os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME", "")

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
def mocked_ses() -> Generator[SESClient, None, None]:
    # Initialize SES client
    with mock_aws():
        try:
            ses: SESClient = boto3.client(
                "ses", region_name=os.getenv("AWS_DEFAULT_REGION")
            )
            ses.verify_email_identity(EmailAddress=os.getenv("SES_NO_REPLY_SENDER", ""))
        except Exception as e:
            pytest.fail(f"Failed to setup ses client and/or verify email identity: {e}")
        yield ses


@pytest.fixture(scope="module", autouse=True)
def mocked_ddb() -> Generator[DynamoDBClient, None, None]:
    # Create table and put items
    db_path: str = os.getenv("TEST_EXAMPLE_DB_PATH", "")
    logger.warning(db_path)
    with mock_aws():
        try:
            table_name = os.getenv("TEMPLATE_METADATA_TABLE_NAME", "")

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

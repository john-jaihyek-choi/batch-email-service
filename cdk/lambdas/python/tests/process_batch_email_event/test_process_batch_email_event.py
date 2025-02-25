# stdlib
import os
import logging
from typing import Dict, Any, List, cast
from http import HTTPStatus

# external libraries
import pytest
from pytest import FixtureRequest
from dotenv import load_dotenv
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_sqs.type_defs import ReceiveMessageResultTypeDef, MessageTypeDef
from aws_lambda_powertools.utilities.data_classes import SQSEvent

# local modules
from process_batch_email_event.main import lambda_handler as process_batch_email_event
from send_batch_email_event.main import lambda_handler as send_batch_email_event
from tests.types import (
    S3EventRecordPayload,
    GenerateMockS3LambdaEventFunction,
)

load_dotenv()


logger = logging.getLogger(__name__)
aws_default_region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
test_queue_name = os.getenv("batch-email-service_email-batch-queue")


# Test Cases
@pytest.mark.parametrize(
    "events, expected_message, http_status",
    [
        (
            "valid_message_events",
            "Messages processed successfully",
            HTTPStatus.OK,
        ),
    ],
)
def test_valid_events(
    request: FixtureRequest,
    events,
    expected_message,
    http_status,
    mocked_sqs: SQSClient,
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
):
    send_batch_email_event_payload = generate_send_batch_email_event_payload(
        generate_mock_s3_lambda_event
    )
    send_batch_email_event(send_batch_email_event_payload)

    queue_url = mocked_sqs.get_queue_url(QueueName=os.getenv("EMAIL_BATCH_QUEUE_NAME"))[
        "QueueUrl"
    ]
    message: ReceiveMessageResultTypeDef = mocked_sqs.receive_message(
        QueueUrl=queue_url
    )

    response = process_batch_email_event(
        transform_sqs_message_to_lambda_event(message["Messages"]), {}
    )

    assert response["StatusCode"] == http_status
    assert response["Message"] == expected_message


def generate_send_batch_email_event_payload(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:

    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "batch/send/valid-recipients-list-1.csv",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Put",
        }
    ]

    return generate_mock_s3_lambda_event(records)


def transform_sqs_message_to_lambda_event(messages: List[MessageTypeDef]) -> SQSEvent:
    transformed_event = {"Records": []}

    for message in messages:
        transformed_event["Records"].append(
            {
                "messageId": message.get("MessageId"),
                "receiptHandle": message.get("receiptHandle"),
                "body": message.get("Body"),
                "attributes": message.get("Attributes"),
                "messageAttributes": message.get("MessageAttributes"),
                "md5OfBody": message.get("MD5OfBody"),
                "eventSource": "aws:sqs",
                "eventSourceARN": f"arn:aws:sqs:{aws_default_region}:123456789012:{test_queue_name}",
                "awsRegion": aws_default_region,
            }
        )

    return cast(SQSEvent, transformed_event)

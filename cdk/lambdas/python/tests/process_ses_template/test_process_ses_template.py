# stdlib
import os
import logging
from typing import Dict, Any, List
from http import HTTPStatus

# external libraries
import pytest
from pytest import FixtureRequest
from dotenv import load_dotenv

# local modules
from process_ses_template.main import lambda_handler
from tests.types import S3EventRecordPayload, GenerateMockS3LambdaEventFunction

load_dotenv()


logger = logging.getLogger(__name__)


# Test Cases
@pytest.mark.parametrize(
    "valid_events, expected_message, http_status",
    [
        ("valid_template_put", "Template update completed successfully", HTTPStatus.OK),
        (
            "valid_template_post",
            "Template update completed successfully",
            HTTPStatus.OK,
        ),
        (
            "valid_template_copy",
            "Template update completed successfully",
            HTTPStatus.OK,
        ),
        (
            "valid_template_multipart_upload",
            "Template update completed successfully",
            HTTPStatus.OK,
        ),
        (
            "valid_template_delete",
            "Template update completed successfully",
            HTTPStatus.OK,
        ),
        (
            "valid_template_delete_marker",
            "Template update completed successfully",
            HTTPStatus.OK,
        ),
    ],
)
def test_valid_events(
    request: FixtureRequest, valid_events, expected_message, http_status
):
    event = request.getfixturevalue(valid_events)
    response = lambda_handler(event, {})

    assert response["StatusCode"] == http_status
    assert response["Message"] == expected_message


@pytest.fixture
def valid_template_put(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:
    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Put",
        }
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.fixture
def valid_template_post(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:
    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Post",
        },
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.fixture
def valid_template_copy(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:
    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Copy",
        },
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.fixture
def valid_template_multipart_upload(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:
    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:CompleteMultipartUpload",
        },
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.fixture
def valid_template_delete(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:
    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectRemoved:Delete",
        },
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.fixture
def valid_template_delete_marker(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:
    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectRemoved:DeleteMarkerCreated",
        },
    ]

    return generate_mock_s3_lambda_event(records)

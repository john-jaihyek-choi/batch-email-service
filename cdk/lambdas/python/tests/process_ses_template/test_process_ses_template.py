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
from process_ses_template.main import lambda_handler as process_ses_template_handler
from tests.types import S3EventRecordPayload, GenerateMockS3LambdaEventFunction

load_dotenv()


logger = logging.getLogger(__name__)


# Test Cases
@pytest.mark.parametrize(
    "events, expected_message, http_status",
    [
        (
            "valid_template_create_events",
            "Template update completed successfully",
            HTTPStatus.OK,
        ),
        (
            "valid_template_remove_events",
            "Template update completed successfully",
            HTTPStatus.OK,
        ),
    ],
)
def test_valid_events(
    request: FixtureRequest,
    events,
    expected_message,
    http_status,
):
    event = request.getfixturevalue(events)
    response = process_ses_template_handler(event, {})

    assert response["StatusCode"] == http_status
    assert response["Message"] == expected_message


@pytest.fixture
def valid_template_create_events(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:
    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/system/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Put",
        },
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/system/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Post",
        },
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/system/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Copy",
        },
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/system/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:CompleteMultipartUpload",
        },
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.fixture
def valid_template_remove_events(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:
    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/system/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectRemoved:Delete",
        },
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/system/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectRemoved:DeleteMarkerCreated",
        },
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.mark.parametrize(
    "events, expected_message, http_status",
    [
        (
            "empty_event",
            "Invalid event: Missing 'Records' key",
            HTTPStatus.BAD_REQUEST,
        ),
        (
            "unsupported_event_type",
            "",
            HTTPStatus.NO_CONTENT,
        ),
    ],
)
def test_invalid_events(request: FixtureRequest, events, expected_message, http_status):
    event = request.getfixturevalue(events)
    response = process_ses_template_handler(event, {})

    assert response["StatusCode"] == http_status
    assert response["Message"] == expected_message


@pytest.fixture
def empty_event(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> Dict[str, Any]:

    return {}


@pytest.fixture
def unsupported_event_type(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> None:

    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "templates/system/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectRestore:Post",
        }
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.mark.parametrize(
    "events, expected_message, http_status",
    [
        (
            "incorrect_bucket",
            "",
            HTTPStatus.NO_CONTENT,
        ),
        (
            "incorrect_object_key",
            "",
            HTTPStatus.NO_CONTENT,
        ),
    ],
)
def test_s3_error_events(
    request: FixtureRequest, events, expected_message, http_status
):
    event = request.getfixturevalue(events)
    response = process_ses_template_handler(event, {})

    assert response["StatusCode"] == http_status
    assert response["Message"] == expected_message


@pytest.fixture
def incorrect_bucket(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> None:

    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": "non-existent-bucket-name",
            "object_key": "templates/system/post-card-combined-template.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Put",
        },
    ]

    return generate_mock_s3_lambda_event(records)


@pytest.fixture
def incorrect_object_key(
    generate_mock_s3_lambda_event: GenerateMockS3LambdaEventFunction,
) -> None:

    records: List[S3EventRecordPayload] = [
        {
            "bucket_name": os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME"),
            "object_key": "prefix/non-existent-key.html",
            "bucket_region": os.getenv("AWS_DEFAULT_REGION"),
            "event_name": "ObjectCreated:Put",
        }
    ]

    return generate_mock_s3_lambda_event(records)

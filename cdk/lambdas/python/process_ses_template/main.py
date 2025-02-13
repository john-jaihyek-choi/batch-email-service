# stdlib
import json
import logging
from typing import Dict, Any, List, Optional
from http import HTTPStatus

# external libraries
from aws_lambda_powertools.utilities.data_classes import S3Event
from aws_lambda_powertools.utilities.typing import LambdaContext

# custom module
from process_ses_template_config import (
    config,
)
from process_ses_template_processor import (
    process_targets,
)
from jc_custom.utils import generate_handler_response, filter_s3_targets, S3Target

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)
logger.warning("running ses main")


def lambda_handler(
    event: S3Event, context: Optional[LambdaContext] = None
) -> Dict[str, Any]:
    try:
        if not event or not event.get("Records"):
            raise ValueError("Invalid event: Missing 'Records' key")

        logger.info("event: %s", json.dumps(event, indent=2))

        allowed_buckets = tuple([config.BATCH_EMAIL_SERVICE_BUCKET_NAME])
        allowed_prefix = tuple(["templates/"])  # prefix must have trailing "/""
        allowed_suffix = tuple([".html", ".txt"])
        allowed_s3_events = tuple(["ObjectCreated", "ObjectRemoved"])

        target_objects: List[S3Target] = filter_s3_targets(
            event, allowed_buckets, allowed_prefix, allowed_suffix, allowed_s3_events
        )

        response = process_targets(target_objects)

        return response

    except ValueError as e:
        logger.exception(f"Value Error: {e}")
        return generate_handler_response(HTTPStatus.BAD_REQUEST.value, str(e))

    except Exception as e:
        logger.exception(f"Critical error in lambda_handler: {str(e)}")
        return generate_handler_response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
            message="An error occurred while processing the batch",
        )

# stdlib
import json
import logging
from typing import Dict, Any, List, Optional
from http import HTTPStatus

# external libraries
from aws_lambda_powertools.utilities.data_classes import S3Event
from aws_lambda_powertools.utilities.typing import LambdaContext

# custom modules
from send_batch_email_event.send_batch_email_event_config import config
from send_batch_email_event.send_batch_email_event_processor import process_s3_targets
from jc_custom.utils import (
    filter_s3_targets,
    generate_handler_response,
    S3Target,
    GenerateHandlerResponseReturnType,
)

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def lambda_handler(
    event: S3Event, context: Optional[LambdaContext] = None
) -> GenerateHandlerResponseReturnType:
    try:
        if not event or not event.get("Records"):
            raise ValueError("Invalid event: Missing 'Records' key")

        logger.info("event: %s", json.dumps(event, indent=2))

        allowed_buckets = tuple([config.BATCH_EMAIL_SERVICE_BUCKET_NAME])
        allowed_prefix = tuple(["batch/send/"])  # prefix must have trailing "/"
        allowed_suffix = tuple([".csv"])
        allowed_s3_events = tuple(["ObjectCreated"])

        target_objects: List[S3Target] = filter_s3_targets(
            event,
            allowed_buckets,
            allowed_prefix,
            allowed_suffix,
            allowed_s3_events,
        )

        if not target_objects:
            return generate_handler_response(
                status_code=HTTPStatus.NO_CONTENT,
                message="No valid s3 targets found",
            )

        logger.info("successfully retrieved all targets from event")
        logger.debug("target_objects: %s", json.dumps(target_objects, indent=2))

        return process_s3_targets(target_objects)

    except ValueError as e:
        logger.exception(f"Value Error: {e}")
        return generate_handler_response(HTTPStatus.BAD_REQUEST, str(e))

    except Exception as e:
        logger.exception(f"Critical error in lambda_handler: {str(e)}")
        return generate_handler_response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message="An error occurred while processing the batch",
        )

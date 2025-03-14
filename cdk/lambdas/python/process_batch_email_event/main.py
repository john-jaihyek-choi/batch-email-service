# stdlib
import json
import logging
from typing import Dict, Any, Optional
from http import HTTPStatus

# external libraries
from aws_lambda_powertools.utilities.data_classes import SQSEvent
from aws_lambda_powertools.utilities.typing import LambdaContext

# custom module
from jc_custom.utils import (
    generate_handler_response,
    filter_sqs_event,
    GenerateHandlerResponseReturnType,
)
from process_batch_email_event_config import config
from process_batch_email_event_processor import process_sqs_message_targets

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def lambda_handler(
    event: SQSEvent, context: Optional[LambdaContext] = None
) -> GenerateHandlerResponseReturnType:
    logger.info("event: %s", json.dumps(event, indent=2))

    try:
        if not event or not event.get("Records"):
            raise ValueError("Invalid event: Missing 'Records' key")

        target_messages = filter_sqs_event(event)

        if not target_messages:
            return generate_handler_response(
                status_code=HTTPStatus.NO_CONTENT,
                message="No valid message targets found",
            )

        logger.debug(
            f"start processing target_messages: {json.dumps(target_messages, indent=2)}"
        )

        return process_sqs_message_targets(target_messages)

    except ValueError as e:
        logger.exception(f"Value Error: {e}")
        return generate_handler_response(HTTPStatus.BAD_REQUEST, str(e))

    except Exception as e:
        logger.exception(f"Critical error in lambda_handler: {str(e)}")
        return generate_handler_response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            message="An error occurred while processing the batch",
        )

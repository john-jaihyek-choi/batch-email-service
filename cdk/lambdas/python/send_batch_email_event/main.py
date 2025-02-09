import json
import logging
from .config import config
from typing import Dict, Any, Optional
from http import HTTPStatus
from jc_shared.utils import generate_handler_response
from .processor import process_event
from aws_lambda_powertools.utilities.data_classes import S3Event
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def lambda_handler(
    event: S3Event, context: Optional[LambdaContext] = None
) -> Dict[str, Any]:
    try:
        if not event or not event.get("Records"):
            raise ValueError("Invalid event: Missing 'Records' key")

        logger.info("event: %s", json.dumps(event, indent=2))

        response = process_event(event)

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

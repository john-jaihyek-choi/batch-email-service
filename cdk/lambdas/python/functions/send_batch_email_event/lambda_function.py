import json
import os
import logging
from typing import Dict, Any, List
from http import HTTPStatus
from collections import OrderedDict, defaultdict
from utils import generate_response
from processor import process_event

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def lambda_handler(event: Dict[str, Any], context: Dict[Any, Any] = None):
    try:
        if not event or not event.get("Records"):  # handle invalid events
            raise ValueError("Invalid event: Missing 'Records' key")

        logger.info("event: %s", json.dumps(event, indent=2))

        response = process_event(event)

        return response

    except ValueError as e:
        logger.exception(f"Value Error: {e}")
        return generate_response(HTTPStatus.BAD_REQUEST.value, str(e))

    except Exception as e:
        logger.exception(f"Critical error in lambda_handler: {str(e)}")
        return generate_response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
            message="An error occurred while processing the batch",
        )

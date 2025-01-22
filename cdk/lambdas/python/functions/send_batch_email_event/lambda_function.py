import json
import os
import logging
from typing import Dict, Any, List
from http import HTTPStatus
from dotenv import load_dotenv
from collections import OrderedDict
from utils import (
    generate_response,
    format_and_filter_targets,
    generate_csv,
)
from boto3_helper import process_targets, send_email_to_admin

# for local executions
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(level=os.getenv("LOG_LEVEL"))
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Dict[Any, Any] = None):
    logger.info("event: %s", event)
    try:
        if not event or not event.get("Records"):  # handle invalid events
            raise ValueError("Invalid event: Missing 'Records' key")

        # retrieve all target s3 objects and store it in target_objects array for further processing
        target_objects: List[Dict[str, str]] = format_and_filter_targets(event)

        if not target_objects:
            return generate_response(
                status_code=HTTPStatus.NO_CONTENT.value,
                message="Target valid targets found",
            )

        target_errors, successful_recipients_count = [], 0

        logger.info("successfully retrieved all targets from event")
        logger.debug("target_objects: %s", json.dumps(target_objects, indent=2))

        for target in target_objects:  # open csv target and organize by N batch
            try:
                batch = process_targets(target)

                # add batch_errors to target_errors if any
                if batch.get("ErrorCount") > 0:
                    target_errors.append(
                        {
                            "Target": batch.get("Target"),
                            "Errors": batch.get("Errors"),
                            "ErrorCount": batch.get("ErrorCount"),
                        }
                    )
                successful_recipients_count += batch.get("SuccessCount", 0)

            except Exception as e:
                logger.error(f"Error processing target {target}: {e}")
                target_errors.append({"Target": target, "Error": str(e)})

        if target_errors:  # handle response with target errors
            error_csv: Dict[str, str] = OrderedDict()

            for error in target_errors:  # generate unique csv per target error
                headers = error.get("Errors")[0].keys()
                target = error.get("Target")
                csv_content = generate_csv(headers, error.get("Errors"))
                error_csv[target] = csv_content

            send_email_to_admin(
                "Batch Email Service - Email Delivery Failed",
                f"Following csv targets had issues:\n\n -{"\n -".join(filename.split("/")[-1] for filename in error_csv.keys())}\n",
                error_csv,
            )

            if successful_recipients_count:  # handle partial success case
                logger.info("partially processed the batches")
                return generate_response(
                    status_code=HTTPStatus.PARTIAL_CONTENT.value,
                    message="Batch partially processed",
                    body={"FailedBatches": target_errors},
                )

            # handle failed batch case
            logger.info("Failed processing the batches")
            return generate_response(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
                message="Failed processing the batches",
                body={"FailedBatches": target_errors},
            )
        else:
            # handle successful batch processing
            logger.info("successfully processed the batches")

            return generate_response(
                HTTPStatus.OK.value, "Batch processing completed successfully"
            )

    except ValueError as e:
        logger.exception(f"Value Error: {e}")
        return generate_response(HTTPStatus.BAD_REQUEST.value, str(e))

    except Exception as e:
        logger.exception(f"Critical error in lambda_handler: {str(e)}")
        return generate_response(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
            message="An error occurred while processing the batch",
        )

import json
import os
import logging
from typing import Dict, Any, List
from http import HTTPStatus
from collections import OrderedDict, defaultdict
from utils import (
    generate_response,
    format_and_filter_targets,
    generate_csv,
    generate_email_template,
    process_targets,
    generate_target_errors_payload,
)
from boto3_helper import send_email_to_admin, move_s3_objects

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def lambda_handler(event: Dict[str, Any], context: Dict[Any, Any] = None):
    try:
        if not event or not event.get("Records"):  # handle invalid events
            raise ValueError("Invalid event: Missing 'Records' key")

        logger.info("event: %s", json.dumps(event, indent=2))

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
                logger.info(f"Target processing: {json.dumps(target, indent=2)}")
                batch = process_targets(target)
            except Exception as e:
                logger.exception(f"Error processing target {target}: {e}")
                target_errors.append(
                    generate_target_errors_payload(
                        target=batch.get("Target"),
                        error_detail=str(e),
                        error_batch=batch.get("Errors"),
                        error_count=batch.get("ErrorCount"),
                        success_count=batch.get("SuccessCount"),
                    )
                )

            # add batch_errors to target_errors if any
            if batch.get("Errors"):
                error_count = batch.get("ErrorCount", 0)
                success_count = batch.get("SuccessCount", 0)

                target_errors.append(
                    generate_target_errors_payload(
                        target=batch.get("Target"),
                        error_detail="Error initiating emails",
                        error_batch=batch.get("Errors"),
                        error_count=error_count,
                        success_count=success_count,
                    )
                )

            successful_recipients_count += batch.get("SuccessCount", 0)

        if target_errors:  # handle response with target errors
            error_csv: Dict[str, str] = OrderedDict()

            for error in target_errors:  # generate unique csv per target error
                headers = error.get("Errors")[0].keys()
                target = error.get("Target")
                csv_content = generate_csv(headers, error.get("Errors"))
                error_csv[target] = csv_content

            template_bucket = os.getenv("BATCH_EMAIL_SERVICE_BUCKET_NAME")
            html_template_key = os.getenv("SEND_BATCH_EMAIL_FAILURE_HTML_TEMPLATE_KEY")
            text_template_key = os.getenv("SEND_BATCH_EMAIL_FAILURE_TEXT_TEMPLATE_KEY")

            # generate html email template
            html_body = generate_email_template(
                template_bucket, html_template_key, "html", target_errors
            )

            # generate text email template (to be attached to the email)
            text_body = generate_email_template(
                template_bucket, text_template_key, "text", target_errors
            )
            error_csv["plain-text-email"] = text_body

            try:
                send_email_to_admin(
                    "Batch Email Service - Email Delivery Failed",
                    html_body,
                    error_csv,
                )
            except Exception as e:
                logger.error(f"Error at send_email_to_admin: {e}")

            if successful_recipients_count:  # handle partial success case
                logger.info("partially processed the batches")
                return generate_response(
                    status_code=HTTPStatus.PARTIAL_CONTENT.value,
                    message="Batch partially processed",
                    body={"FailedBatches": target_errors},
                )

            # handle failed batch case
            try:
                s3_list = defaultdict(list)
                for target in target_errors:
                    source = target["Target"].split("/")
                    bucket = source[0]
                    key = "/".join(source[1:])
                    s3_list[bucket].append({"Key": key})

                move_s3_objects(
                    bucket_list=s3_list,
                )

            except Exception as e:
                logger.exception(f"Error moving s3 objects: {e}")

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

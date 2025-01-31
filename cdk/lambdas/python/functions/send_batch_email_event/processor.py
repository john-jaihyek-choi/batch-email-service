import json
import os
import logging
from config import config
from typing import Dict, Any, List
from http import HTTPStatus
from collections import OrderedDict
from utils import (
    generate_response,
    format_and_filter_targets,
    generate_csv,
    generate_email_template,
    process_targets,
    generate_target_errors_payload,
)
from boto3_helper import send_ses_email, move_s3_objects

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def process_event(event: Dict[str, Any]) -> Dict[str, Any]:
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
        except Exception as e:
            logger.exception(f"Error processing target {target}: {e}")

        if target_errors:  # handle response with target errors
            return handle_target_errors(target_errors, successful_recipients_count)

        # handle successful batch processing
        logger.info("successfully processed the batches")

        return generate_response(
            HTTPStatus.OK.value, "Batch processing completed successfully"
        )


def handle_target_errors(
    target_errors: List[Dict[str, Any]], successful_recipients_count: int
) -> Dict[str, Any]:
    attachments: Dict[str, str] = OrderedDict()

    for error in target_errors:  # generate unique csv per target error
        headers = error.get("Errors")[0].keys()
        target = error.get("Target")
        csv_content = generate_csv(headers, error.get("Errors"))
        attachments[target] = csv_content

    template_bucket = config.BATCH_EMAIL_SERVICE_BUCKET_NAME
    html_template_key = config.SEND_BATCH_EMAIL_FAILURE_HTML_TEMPLATE_KEY
    text_template_key = config.SEND_BATCH_EMAIL_FAILURE_TEXT_TEMPLATE_KEY

    # generate html email template
    html_body = generate_email_template(
        template_bucket, html_template_key, "html", target_errors
    )

    # generate text email template (to be attached to the email)
    text_body = generate_email_template(
        template_bucket, text_template_key, "text", target_errors
    )
    attachments["plain-text-email"] = text_body

    send_ses_email(
        send_from=config.SES_NO_REPLY_SENDER,
        send_to=config.SES_ADMIN_EMAIL,
        subject="Batch Email Service - Email Initiation Failed",
        body=html_body,
        attachments=attachments,
    )

    if successful_recipients_count:  # handle partial success case
        logger.info("partially processed the batches")
        return generate_response(
            status_code=HTTPStatus.PARTIAL_CONTENT.value,
            message="Batch partially processed",
            body={"FailedBatches": target_errors},
        )

    move_failed_objects(target_errors)

    logger.info("Failed processing the batches")
    return generate_response(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
        message="Failed processing the batches",
        body={"FailedBatches": target_errors},
    )


def move_failed_objects(target_errors: List[Dict[str, Any]]):
    # handle failed batch case
    try:
        s3_list = []
        for target in target_errors:
            source = target["Target"].split("/")
            bucket, file_name = source[0], source[-1]

            key = "/".join(source[1:])
            s3_list.append(
                {
                    "From": {
                        "Bucket": bucket,
                        "Key": key,
                    },
                    "To": {
                        "Bucket": bucket,
                        "Key": f"{config.BATCH_INITIATION_ERROR_S3_PREFIX}/{file_name}",
                    },
                }
            )

        logger.info(f"Moving s3 objects: {s3_list}")
        move_s3_objects(
            targets=s3_list,
        )

    except Exception as e:
        logger.exception(f"Error moving s3 objects: {e}")

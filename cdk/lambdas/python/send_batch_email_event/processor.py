import json
import logging
from .config import config
from typing import Dict, Any, List, Literal, Optional
from http import HTTPStatus
from collections import OrderedDict
from .utils import (
    format_and_filter_targets,
    generate_email_template,
    process_targets,
    generate_target_errors_payload,
)
from mypy_boto3_s3.type_defs import CopySourceTypeDef
from jc_shared.utils import generate_handler_response, generate_csv
from jc_shared.boto3_helper import (
    send_ses_email,
    move_s3_objects,
)
from aws_lambda_powertools.utilities.data_classes import S3Event

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def process_event(event: S3Event) -> Dict[str, Any]:
    target_objects = format_and_filter_targets(event)

    if not target_objects:
        return generate_handler_response(
            status_code=HTTPStatus.NO_CONTENT.value,
            message="No valid targets found",
        )

    target_errors: List[Dict[str, Any]] = []
    successful_recipients_count = 0

    logger.info("successfully retrieved all targets from event")
    logger.debug("target_objects: %s", json.dumps(target_objects, indent=2))

    for target in target_objects:  # open csv target and organize by N batch
        try:
            logger.info(f"Target processing: {json.dumps(target, indent=2)}")
            batch = process_targets(target)
            target_path: str = batch.get("Target", "unknown target")
            error_batch: List[Dict[str, Any]] = batch.get("Errors", [])

            if batch.get("Errors"):
                error_count = batch.get("ErrorCount", 0)
                success_count = batch.get("SuccessCount", 0)

                target_errors.append(
                    generate_target_errors_payload(
                        target=target_path,
                        error_detail="Error initiating emails",
                        error_batch=error_batch,
                        error_count=error_count,
                        success_count=success_count,
                    )
                )

            successful_recipients_count += batch.get("SuccessCount", 0)
        except Exception as e:
            logger.exception(f"Error processing target {target}: {e}")

    if target_errors:
        return handle_target_errors(target_errors, successful_recipients_count)

    logger.info("successfully processed the batches")

    return generate_handler_response(
        HTTPStatus.OK.value, "Batch processing completed successfully"
    )


def handle_target_errors(
    target_errors: List[Dict[str, Any]], successful_recipients_count: int
) -> Dict[str, Any]:
    attachments: OrderedDict[str, str] = OrderedDict()
    fields: Dict[str, str] = target_errors[0].get("Errors", [])[0]
    headers = fields.keys()

    for i, error in enumerate(target_errors):  # generate unique csv per target error
        target = error.get("Target", f"unknown-target-{i}")
        csv_content = generate_csv(headers, error.get("Errors", []))
        attachments[target] = csv_content

    template_bucket = config.BATCH_EMAIL_SERVICE_BUCKET_NAME
    html_template_key = config.SEND_BATCH_EMAIL_FAILURE_HTML_TEMPLATE_KEY
    text_template_key = config.SEND_BATCH_EMAIL_FAILURE_TEXT_TEMPLATE_KEY

    # generate html email template (to be attached to the email)
    body = generate_email_template(
        template_bucket, html_template_key, target_errors, "html"
    )

    # generate text email template (to be attached to the email)
    attachment_body = generate_email_template(
        template_bucket, text_template_key, target_errors, "txt"
    )
    attachments["plain-text-email"] = attachment_body

    send_ses_email(
        send_from=config.SES_NO_REPLY_SENDER,
        send_to=config.SES_ADMIN_EMAIL,
        subject="Batch Email Service - Email Initiation Failed",
        body=body,
        attachments=attachments,
        body_type="html",
    )

    if (
        successful_recipients_count
    ):  # handle partial success case (x out of total recipients successful scenario)
        logger.info("partially processed the batches")
        return generate_handler_response(
            status_code=HTTPStatus.PARTIAL_CONTENT.value,
            message="Batch partially processed",
            body={"FailedBatches": target_errors},
        )

    move_failed_objects(target_errors)

    logger.info("Failed processing the batches")
    return generate_handler_response(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value,
        message="Failed processing the batches",
        body={"FailedBatches": target_errors},
    )


def move_failed_objects(target_errors: List[Dict[str, Any]]):
    try:
        s3_list: List[Dict[Literal["From", "To"], CopySourceTypeDef]] = []
        for target in target_errors:
            source = target["Target"].split("/")
            bucket, file_name = source[0], source[-1]

            key = "/".join(source[1:])
            # organize from and to bucket objects in a list
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

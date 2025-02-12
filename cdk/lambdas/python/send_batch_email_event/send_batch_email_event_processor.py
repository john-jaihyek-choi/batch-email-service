# stdlib
import json
import logging
from typing import Dict, Any, List, Literal
from http import HTTPStatus
from collections import OrderedDict

# external libraries
from mypy_boto3_s3.type_defs import CopySourceTypeDef

# custom modules
from send_batch_email_event_config import config
from send_batch_email_event_utils import (
    process_batch,
    generate_target_errors_payload,
    generate_template_replacement_pattern,
)
from jc_custom.utils import (
    autofill_email_template,
    generate_handler_response,
    generate_csv,
    S3Target,
)
from jc_custom.boto3_helper import send_ses_email, move_s3_objects, get_s3_object

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def process_targets(target_objects: List[S3Target]) -> Dict[str, Any]:
    target_errors: List[Dict[str, Any]] = []
    successful_recipients_count = 0

    for target in target_objects:  # open csv target and organize by N batch
        try:
            logger.info(f"Target processing: {json.dumps(target, indent=2)}")
            batch = process_batch(target)
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

    html_template_replacements = generate_template_replacement_pattern(
        target_errors, "html"
    )
    text_template_replacements = generate_template_replacement_pattern(
        target_errors, "txt"
    )

    html_body = autofill_email_template(
        get_s3_object(template_bucket, html_template_key),
        html_template_replacements,
    )
    txt_body = autofill_email_template(
        get_s3_object(template_bucket, text_template_key),
        text_template_replacements,
    )

    attachments["plain-text-email"] = txt_body

    logger.info("sending ses email...")

    send_ses_email(
        send_from=config.SES_NO_REPLY_SENDER,
        send_to=config.SES_ADMIN_EMAIL,
        subject="Batch Email Service - Email Initiation Failed",
        body=html_body,
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

    logger.info(f"moving failed object s3 location {target_errors}")

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
                        "Key": config.BATCH_INITIATION_ERROR_S3_PREFIX + file_name,
                    },
                }
            )

        logger.info(f"Moving s3 objects: {s3_list}")
        move_s3_objects(
            targets=s3_list,
        )

    except Exception as e:
        logger.exception(f"Error moving s3 objects: {e}")

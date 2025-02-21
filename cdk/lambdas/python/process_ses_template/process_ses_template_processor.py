# stdlib
import logging
import json
import re
from typing import Dict, Any, List, Literal, Optional, TypedDict
from http import HTTPStatus

# custom modules
from jc_custom.boto3_helper import (
    get_s3_object,
    delete_ddb_item,
    put_ddb_item,
    send_ses_email,
)
from process_ses_template_config import (
    config,
)
from jc_custom.utils import S3Target, autofill_email_template, generate_handler_response

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class ProcessedTarget(TypedDict):
    success: bool
    target: str
    message: Optional[str]


TemplateMetadataFields = List[Dict[Literal["S"], str]]


def process_targets(s3_targets: List[S3Target]) -> Dict[str, Any]:
    table_name = config.TEMPLATE_METADATA_TABLE_NAME
    target_processed: List[Dict[str, Any]] = []

    for target in s3_targets:
        bucket_name, prefix, object, event_name = (
            target.get("BucketName"),
            target.get("Prefix"),
            target.get("Object"),
            target.get("EventName"),
        )
        key = prefix + object

        try:
            if event_name.startswith(
                "ObjectRemoved"
            ):  # treating Removal operations uniquely
                logger.info(f"removing {key}...")

                delete_ddb_item(table_name=table_name, key={"template_key": {"S": key}})

                target_processed.append(
                    {"success": True, "target": key, "message": "Delete success"}
                )

                logger.info(f"{key} removal complete!")
            else:  # all create/put operations
                logger.info(f"extracting required fields from {object}...")

                template_fields = extract_required_fields_from_s3(
                    bucket_name=bucket_name, object_key=key
                )

                logger.info(
                    f"successfully extracted fields from {object}: {template_fields}"
                )

                logger.info(f"updating {table_name}...")

                put_ddb_item(
                    table_name=table_name,
                    item={"template_key": {"S": key}, "fields": {"L": template_fields}},
                )

                target_processed.append(
                    {"success": True, "target": key, "message": "Upload success"}
                )

                logger.info(f"{key} update complete!")

        except Exception as e:
            logger.exception(f"Failed to update {table_name}: {e}")
            target_processed.append(
                {
                    "success": False,
                    "target": key,
                    "message": "Error updating template metadata table",
                }
            )
            continue

    send_ses_template_status_report(target_processed)

    successfully_processed = sum(target["success"] for target in target_processed)

    if successfully_processed == 0:  # no successfully processed target
        logger.info("Template process failed")
        return generate_handler_response(
            HTTPStatus.INTERNAL_SERVER_ERROR.value, "Template update failed"
        )
    elif successfully_processed != len(target_processed):  # partially successful
        logger.info("Template update partially completed")
        return generate_handler_response(
            HTTPStatus.OK.value, "Template update partially successful"
        )
    else:
        logger.info(f"Upload status email sent successfully via SES!")

        return generate_handler_response(
            HTTPStatus.OK.value, "Template update completed successfully"
        )


def send_ses_template_status_report(processed_targets: List[Dict[str, Any]]) -> None:
    (
        batch_email_service_bucket_name,
        admin_email_html_template_key,
        admin_email_txt_template_key,
    ) = (
        config.BATCH_EMAIL_SERVICE_BUCKET_NAME,
        config.PROCESS_SES_TEMPLATE_FAILURE_HTML_TEMPLATE_KEY,
        config.PROCESS_SES_TEMPLATE_FAILURE_TEXT_TEMPLATE_KEY,
    )

    html_fields_mapping = generate_template_mapping(processed_targets, "html")
    txt_fields_mapping = generate_template_mapping(processed_targets, "txt")

    ses_html_body = autofill_email_template(
        get_s3_object(batch_email_service_bucket_name, admin_email_html_template_key),
        html_fields_mapping,
    )

    attachments = {
        "plain-text-email": autofill_email_template(
            get_s3_object(
                batch_email_service_bucket_name, admin_email_txt_template_key
            ),
            txt_fields_mapping,
        )
    }

    send_ses_email(
        send_from=config.SES_NO_REPLY_SENDER,
        send_to=config.SES_ADMIN_EMAIL,
        subject="Template Update Status Report",
        body=ses_html_body,
        attachments=attachments,
        body_type="html",
    )


def extract_required_fields_from_s3(
    bucket_name: str, object_key: str
) -> TemplateMetadataFields:
    logger.debug(f"extracting {object_key} from {bucket_name}")

    template = get_s3_object(bucket_name=bucket_name, object_key=object_key)

    logger.debug(f"template retrieved ({object_key}): {template}")

    # extract group of texts in {{}}
    required_fields = set(re.findall(r"\{\{(.*?)\}\}", template))

    logger.debug(f"dynamic fields: {template}")

    return [{"S": field} for field in required_fields]


def generate_template_mapping(
    target_processed: List[ProcessedTarget], for_type: Literal["html", "txt"]
):
    success_count, total_count = 0, len(target_processed)
    template_process_details: List[str] = []

    for target in target_processed:
        template_key, success, message = (
            target.get("target", "unknown-template"),
            target.get("success", False),
            target.get("message", ""),
        )
        success_count += success

        if for_type == "html":
            template_process_details.append(f"<li>{template_key} - {message}</li>")
        else:
            template_process_details.append(f"{template_key} - {message}")

    aggregate_error_rate = round((total_count - success_count) / total_count * 100)
    aggregate_success_rate = 100 - aggregate_error_rate

    aggregate_success_text = (
        f'<div class="bar-success" style="width: {aggregate_success_rate}%">{aggregate_success_rate}%</div>'
        if aggregate_success_rate
        else ""
    )
    aggregate_error_text = (
        f'<div class="bar-failed" style="width: {aggregate_error_rate}%">{aggregate_error_rate}%</div>'
        if aggregate_error_rate
        else ""
    )

    return {
        "template_update_result": "".join(template_process_details),
        "aggregate_error_rate": str(aggregate_error_rate),
        "aggregate_success_rate": str(100 - aggregate_error_rate),
        "aggregate_success_text": (
            aggregate_success_text if for_type == "html" else aggregate_success_rate
        ),
        "aggregate_error_text": (
            aggregate_error_text if for_type == "html" else aggregate_error_rate
        ),
    }

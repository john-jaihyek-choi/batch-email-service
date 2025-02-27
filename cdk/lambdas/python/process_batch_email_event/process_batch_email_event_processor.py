# stdlib
import logging
import json
import re
from typing import Dict, Any, List, Literal, Optional, TypedDict
from http import HTTPStatus

# external libraries
from boto3.dynamodb.types import TypeDeserializer

# custom modules
from jc_custom.boto3_helper import (
    get_s3_object,
    send_ses_email,
)
from process_batch_email_event_config import (
    config,
)
from jc_custom.utils import SQSMessageTarget, autofill_email_template
from jc_custom.boto3_helper import update_ddb_item

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

deserializer = TypeDeserializer()


def update_ddb_batch_details_field(
    batch_name: str,
    failed: List[Dict[str, str]],
    successful: List[Dict[str, str]],
) -> Any:
    table_name = config.EMAIL_BATCH_TRACKER_TABLE_NAME

    logger.debug(f"updating batch_details field in {table_name}")

    res = update_ddb_item(
        table_name=table_name,
        key={"batch_name": {"S": batch_name}},
        update_expression="""
            SET batch_details.failed = list_append(batch_details.failed, :failed)
            SET batch_details.success = list_append(batch_details.success, :successful)
            SET batch_processed = batch_processed + :plusone
        """,
        expression_attribute_values={
            ":failed": {
                "L": [
                    {"M": {key: {"S": str(val)} for key, val in recipient.items()}}
                    for recipient in failed
                ]
            },
            ":successful": {
                "L": [
                    {"M": {key: {"S": str(val)} for key, val in recipient.items()}}
                    for recipient in successful
                ]
            },
            ":plusone": {"N": "1"},
        },
        return_values="ALL_NEW",
    )

    return res


def process_recipients(target: SQSMessageTarget):
    failed_recipients, successful_recipients = [], []

    for recipient in target["Recipients"]:
        try:
            send_from, send_to, template_key, subject = (
                recipient["send_from"],
                recipient["send_to"],
                recipient["email_template"],
                recipient["subject"],
            )
            template_type = template_key.split(".")[-1]

            logger.debug(f"getting html_email_template from {template_key}")

            html_email_template = get_s3_object(
                config.BATCH_EMAIL_SERVICE_BUCKET_NAME, template_key
            )

            logger.debug(f"autofilling email template...")

            html_body = autofill_email_template(
                template=html_email_template, replacement_mapping=recipient
            )

            logger.debug(f"sending email... {recipient}")

            send_ses_email(
                send_from, send_to, subject, html_body, body_type=template_type
            )

            successful_recipients.append(recipient)

            logger.debug("Email sent!")

        except Exception as e:
            logger.exception(f"failed to process target, skipping to next target: {e}")
            failed_recipient = recipient.copy()
            failed_recipient["error"] = str(e)

            failed_recipients.append(failed_recipient)

    return {
        "failed_recipients": failed_recipients,
        "successful_recipients": successful_recipients,
    }

# stdlib
import os
import logging
from typing import List
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class Config:
    EMAIL_BATCH_QUEUE_NAME: str = os.getenv("EMAIL_BATCH_QUEUE_NAME", "")
    RECIPIENTS_PER_MESSAGE: int = int(os.getenv("RECIPIENTS_PER_MESSAGE", 50))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    AWS_DEFAULT_REGION: str = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
    SES_NO_REPLY_SENDER: str = os.getenv("SES_NO_REPLY_SENDER", "")
    SES_ADMIN_EMAIL: str = os.getenv("SES_ADMIN_EMAIL", "")
    BATCH_EMAIL_SERVICE_BUCKET_NAME: str = os.getenv(
        "BATCH_EMAIL_SERVICE_BUCKET_NAME", ""
    )
    SEND_BATCH_EMAIL_FAILURE_HTML_TEMPLATE_KEY: str = os.getenv(
        "SEND_BATCH_EMAIL_FAILURE_HTML_TEMPLATE_KEY", ""
    )
    SEND_BATCH_EMAIL_FAILURE_TEXT_TEMPLATE_KEY: str = os.getenv(
        "SEND_BATCH_EMAIL_FAILURE_TEXT_TEMPLATE_KEY", ""
    )
    BATCH_INITIATION_ERROR_S3_PREFIX: str = os.getenv(
        "BATCH_INITIATION_ERROR_S3_PREFIX", ""
    )
    EMAIL_REQUIRED_FIELDS: List[str] = os.getenv("EMAIL_REQUIRED_FIELDS", "").split(",")
    TEMPLATE_METADATA_TABLE_NAME: str = os.getenv("TEMPLATE_METADATA_TABLE_NAME", "")
    EMAIL_BATCH_TRACKER_TABLE_NAME: str = os.getenv(
        "EMAIL_BATCH_TRACKER_TABLE_NAME", ""
    )


config = Config()

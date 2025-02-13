# stdlib
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class Config:
    LOG_LEVEL: Optional[str] = os.getenv("LOG_LEVEL", "INFO")
    AWS_DEFAULT_REGION: Optional[str] = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
    SES_NO_REPLY_SENDER: Optional[str] = os.getenv("SES_NO_REPLY_SENDER")
    SES_ADMIN_EMAIL: Optional[str] = os.getenv("SES_ADMIN_EMAIL")
    BATCH_EMAIL_SERVICE_BUCKET_NAME: str = os.getenv(
        "BATCH_EMAIL_SERVICE_BUCKET_NAME", ""
    )
    TEMPLATE_METADATA_TABLE_NAME: str = os.getenv("TEMPLATE_METADATA_TABLE_NAME", "")
    PROCESS_SES_TEAMPLATE_FAILURE_HTML_TEMPLATE_KEY: str = os.getenv(
        "PROCESS_SES_TEAMPLATE_FAILURE_HTML_TEMPLATE_KEY", ""
    )
    PROCESS_SES_TEAMPLATE_FAILURE_TEXT_TEMPLATE_KEY: str = os.getenv(
        "PROCESS_SES_TEAMPLATE_FAILURE_TEXT_TEMPLATE_KEY", ""
    )
    SES_NO_REPLY_SENDER: str = os.getenv("SES_NO_REPLY_SENDER")
    SES_ADMIN_EMAIL: str = os.getenv("SES_ADMIN_EMAIL")


config = Config()

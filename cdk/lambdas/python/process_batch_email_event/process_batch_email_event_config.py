# stdlib
import os
import logging
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class Config:
    LOG_LEVEL: Optional[str] = os.getenv("LOG_LEVEL", "INFO")
    SES_NO_REPLY_SENDER: Optional[str] = os.getenv("SES_NO_REPLY_SENDER")
    SES_ADMIN_EMAIL: str = os.getenv("SES_ADMIN_EMAIL")
    BATCH_EMAIL_SERVICE_BUCKET_NAME: str = os.getenv(
        "BATCH_EMAIL_SERVICE_BUCKET_NAME", ""
    )


config = Config()

# stdlib
import logging
import json
import re
from typing import Dict, Any, List, Literal
from http import HTTPStatus

# custom modules
from jc_custom.boto3_helper import get_s3_object, put_ddb_item
from process_ses_template_config import (
    config,
)
from jc_custom.utils import generate_handler_response, S3Target

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def process_event(s3_targets: List[S3Target]) -> Dict[str, Any]:
    # General steps:
    # 1. get the S3 object target
    # 2. scan through the content and look for dynamic variables
    # 3. update ddb with the metdata
    # 4. send test ses to the admin email
    # 5. return success response

    for target in s3_targets:
        try:
            bucket_name, prefix, object = (
                target.get("BucketName"),
                target.get("Prefix"),
                target.get("Object"),
            )
            key = prefix + object
            table_name = config.TEMPLATE_METADATA_TABLE_NAME

            logger.info(f"extracting required fields from {object}...")

            items = extract_required_fields_from_s3(
                bucket_name=bucket_name, object_key=key
            )

            logger.info(f"successfully extracted fields from {object}: {items}")

            logger.info(f"updating {table_name}...")

            put_ddb_item(
                table_name=table_name,
                item={"template_key": {"S": key}, "fields": {"L": items}},
            )

            logger.info(f"{table_name} update complete!")

        except Exception as e:
            logger.exception(f"Unexpected error processing the event: {e}")

    return generate_handler_response(
        status_code=HTTPStatus.OK.value, message="Successfully processed event"
    )


def extract_required_fields_from_s3(
    bucket_name: str, object_key: str
) -> List[Dict[Literal["S"], str]]:
    try:
        logger.debug(f"extracting {object_key} from {bucket_name}")

        file = get_s3_object(bucket_name=bucket_name, object_key=object_key)
        template = file["Body"].read().decode("utf-8")

        logger.debug(f"template retrieved ({object_key}): {template}")

        # extract group of texts in {{}}
        required_fields = set(re.findall(r"\{\{(.*?)\}\}", template))

        logger.debug(f"dynamic fields: {template}")

        return [{"S": field} for field in required_fields]

    except Exception as e:
        logger.exception(f"Unexpected error extracting required fields... {e}")
        raise

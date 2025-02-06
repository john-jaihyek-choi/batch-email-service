import logging
import csv
import io
import re

from functools import lru_cache
from collections import OrderedDict
from typing import Dict, List, Any, Literal, Optional, cast, IO
from botocore.exceptions import ClientError

from send_batch_email_event.config import config
from jc_custom.boto3_helper import get_s3_object, send_sqs_message, get_ddb_item
from jc_custom.utils import S3Target

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def batch_read_csv(file_obj, batch_size: int):  # reads a CSV file in batches in place
    batch: List[OrderedDict[str, Any]] = []
    row_errors: List[OrderedDict[str, Any]] = []

    csv_reader = csv.DictReader(file_obj)
    if csv_reader.fieldnames is None:
        row_errors.append(
            OrderedDict(
                {
                    "Error": "No headers found",
                }
            )
        )
        yield batch, row_errors

    for row_number, row in enumerate(csv_reader, start=2):
        if not row:
            continue

        try:
            custom_fields = {"row_number": row_number}
            row_info = OrderedDict({**custom_fields, **row})

            basic_fields, template_fields = validate_basic_fields(
                row, config.EMAIL_REQUIRED_FIELDS
            ), validate_template_fields(row, config.TEMPLATE_METADATA_TABLE_NAME)

            if (
                basic_fields or template_fields
            ):  # basic or template specific fields missing
                message = f"Missing {"basic" if basic_fields else ""}{" & " if basic_fields and template_fields else ""}{"template specific" if template_fields else ""} required fields"
                row_errors.append(
                    OrderedDict(
                        {
                            **row_info,
                            "Error": message,
                            "MissingFields": basic_fields + template_fields,
                        }
                    )
                )
            else:  # no missing fields
                batch.append(row_info)
        except ClientError as e:
            response = cast(Dict[str, Any], e.response)

            if response["Error"]["Code"] == "ResourceNotFoundException":
                row_errors.append(
                    OrderedDict({**row_info, "Error": f"Template does not exist {e}"})
                )
        except Exception as e:
            row_errors.append(
                OrderedDict({**row_info, "Error": f"Unidentified error {e}"})
            )

        if len(batch) == batch_size:
            yield batch, []
            batch = []
    else:
        yield batch, row_errors


def generate_email_template(
    s3_bucket: str,
    s3_key: str,
    target_errors: List[Dict[str, Any]],
    template_type: Optional[Literal["html", "txt"]] = "txt",
) -> str:
    try:
        file = get_s3_object(s3_bucket, s3_key)
        template = file["Body"].read().decode("utf-8")

        attachments, batch_success_details = [], []
        aggregate_success_count, aggregate_error_count = 0, 0

        for target in target_errors:
            target_path: str = target.get("Target", "")
            file_name = target_path.split("/")[-1]

            success_count, error_count = target.get("SuccessCount", 0), target.get(
                "ErrorCount", 0
            )
            total_count = success_count + error_count
            aggregate_success_count += success_count
            aggregate_error_count += error_count

            if template_type == "html":
                batch_success_details.append(
                    f"<li>{file_name} – {error_count} of {total_count} rows failed</li>"
                )

                attachments.append(f"<li><a>{file_name}</a></li>")
            else:
                batch_success_details.append(
                    f"- {error_count} of {total_count} rows failed\n"
                )
                attachments.append(f"- {file_name}\n")

        aggregate_total_count = aggregate_success_count + aggregate_error_count
        aggregate_error_rate = round(
            aggregate_error_count / aggregate_total_count * 100
        )
        aggregate_success_rate = round(
            aggregate_success_count / aggregate_total_count * 100
        )

        # replacement key-val pair (key = variables in html, val = value to be replaced to)
        replacements = {
            "{{aggregate_success_rate}}": (f"{aggregate_success_rate}"),
            "{{aggregate_error_rate}}": (f"{aggregate_error_rate}"),
            "{{aggregate_success_text}}": (
                f'<div class="bar-success" style="width: {aggregate_success_rate}%">{aggregate_success_rate}%</div>'
                if aggregate_success_rate
                else ""
            ),
            "{{aggregate_error_text}}": (
                f'<div class="bar-failed" style="width: {aggregate_error_rate}%">{aggregate_error_rate}%</div>'
                if aggregate_error_rate
                else ""
            ),
            "{{attachment_list}}": f"{"".join(attachments)}",
            "{{batch_success_details}}": f"{"".join(batch_success_details)}",
        }

        # replace key-val pairs in replacements from the template
        template = re.sub(
            r"{{(aggregate_success_rate|aggregate_error_rate|aggregate_success_text|aggregate_error_text|attachment_list|batch_success_details)}}",
            lambda match: replacements.get(match.group(0), match.group(0)),
            template,
        )
        logger.debug(template)
        return template
    except Exception as e:
        logger.exception(f"Error generating template: {e}")
        return ""


def generate_target_errors_payload(
    target: str,
    error_detail: str,
    error_batch: List[Dict[str, Any]],
    error_count: int,
    success_count: int,
) -> Dict[str, Any]:
    return {
        "Target": target,
        "Error": error_detail,
        "Errors": error_batch,
        "ErrorCount": error_count,
        "SuccessCount": success_count,
    }


def generate_batch_payload(
    target_path: str, success_count: int, errors: List[Dict[str, Any]], error_count: int
) -> Dict[str, Any]:
    return {
        "Target": target_path,
        "SuccessCount": success_count,
        "Errors": errors,
        "ErrorCount": error_count,
    }


@lru_cache(
    maxsize=config.RECIPIENTS_PER_MESSAGE
)  # allocate cache to decrease api call volume to ddb
def get_template_metadata(ddb_table_name: str, primary_key: str):
    return get_ddb_item(ddb_table_name, primary_key)


def process_targets(s3_target: S3Target) -> Dict[str, Any]:
    try:
        recipients_per_message = config.RECIPIENTS_PER_MESSAGE

        batch_errors, success_count = [], 0

        bucket_name, prefix, object, principal_id, timestamp = (
            s3_target["BucketName"],
            s3_target["Prefix"],
            s3_target["Object"],
            s3_target["PrincipalId"],
            s3_target["Timestamp"],
        )

        target_path = f"{bucket_name}/{prefix}{object}"

        logger.info(f"getting {target_path}...")
        s3_object = get_s3_object(
            bucket_name=bucket_name, object_key=f"{prefix}{object}"
        )

        # wrap s3 object body as IO for it to be read in place
        wrapper = io.TextIOWrapper(cast(IO[bytes], s3_object["Body"]), encoding="utf-8")

        logger.info(f"grouping recipients by {recipients_per_message}...")

        batch_number = 1

        # group the recipients and send message to sqs
        for batch, failed_rows in batch_read_csv(wrapper, recipients_per_message):
            batch_id = f"{target_path}-{timestamp}-{batch_number}"
            batch_number += 1
            success_count += len(batch)

            try:
                if batch:
                    message = {
                        "BatchId": batch_id,
                        "Recipients": batch,
                        "Metadata": {
                            "UploadedBy": principal_id,
                            "Timestamp": timestamp,
                        },
                    }

                    logger.debug(f"sending batch {batch_number}...")

                    send_sqs_message(
                        queue_name=config.EMAIL_BATCH_QUEUE_NAME,
                        message_body=message,
                    )
            except Exception as e:
                logger.exception(
                    f"Failed to send sqs batch {batch_number} for {target_path}: {e}"
                )
                batch_errors.append(
                    {
                        "FailedRecipients": batch,
                        "Error": f"Failed to send batch: {str(e)}",
                    }
                )
        else:  # add to collection of failed rows when done
            logger.debug("adding failed rows to batch_errors!")
            batch_errors.extend(failed_rows)
    except Exception as e:
        logger.exception(f"Error at processing target: {e}")
        batch_errors.append({"Error": "Unexpected error", "Details": str(e)})
    finally:
        return generate_batch_payload(
            target_path,
            success_count,
            errors=batch_errors,
            error_count=len(batch_errors),
        )


def validate_template_fields(row: Dict[str, Any], ddb_table_name) -> List[str]:
    template_key = row.get("email_template")
    missing = []

    try:
        response: Dict[str, Any] = get_template_metadata(ddb_table_name, template_key)
    except Exception as e:
        logger.exception(f"Unexpected error with get_ddb_item: {e}")
        raise

    ddb_item: Dict[str, Any] | None = response.get("Item")

    # check ddb_item is valid, fields column is valid, and field is non-empty
    if ddb_item and ddb_item["fields"] and ddb_item["fields"]["S"]:
        fields: Dict[str, Any] = ddb_item.get("fields", {})
        fields_list: str | None = fields["S"]

        if fields_list:
            for field in fields_list.split(","):
                field = field.strip()
                if field not in row or not row[field]:
                    missing.append(field)

    return missing


def validate_basic_fields(row: Dict[str, Any], required_fields: List[str]) -> List[str]:
    missing = []

    for field in required_fields:
        field = field.strip()
        if field not in row or not row[field]:
            missing.append(field)

    return missing

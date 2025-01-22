import json
import os
import logging
import io
import boto3
import boto3.exceptions
from datetime import datetime
from typing import Dict, Any, List
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from utils import (
    batch_read_csv,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL"))
logger = logging.getLogger(__name__)

aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

sqs = boto3.client("sqs", aws_region)
s3 = boto3.client("s3", aws_region)
ses = boto3.client("ses", aws_region)


def process_targets(s3_target: Dict[str, str]) -> Dict[str, Any]:
    recipients_per_message = int(os.getenv("RECIPIENTS_PER_MESSAGE", 50))

    batch_errors, success_count = [], 0

    try:
        logger.info("processing targets... %s", s3_target)

        bucket_name, prefix, key, principal_id, timestamp = (
            s3_target["BucketName"],
            s3_target["Prefix"],
            s3_target["Key"],
            s3_target["PrincipalId"],
            s3_target["Timestamp"],
        )

        s3_object = s3.get_object(Bucket=bucket_name, Key=f"{prefix}/{key}")
        target_path = f"{bucket_name}/{prefix}/{key}"

        logger.info(f"getting {target_path}...")
        wrapper = io.TextIOWrapper(s3_object["Body"], encoding="utf-8")

        logger.info(f"grouping recipients by {recipients_per_message}...")

        batch_number = 1

        # group the recipients and send message to sqs
        for batch, failed_rows in batch_read_csv(wrapper, recipients_per_message):
            batch_id = f"{bucket_name}/{prefix}/{key}-{timestamp}-{batch_number}"
            batch_number += 1
            success_count += len(batch)

            try:
                if batch:
                    send_sqs_message(
                        batch_id,
                        timestamp,
                        batch_number,
                        principal_id,
                        batch,
                    )
            except Exception as e:
                logger.exception(
                    f"Failed to send batch {batch_number} for {target_path}"
                )
                batch_errors.append(
                    {
                        "FailedRecipients": batch,
                        "Error": f"Failed to send batch: {str(e)}",
                    }
                )
            finally:
                if (
                    failed_rows
                ):  # add to collection of failed rows to batch errors if any
                    batch_errors.extend(failed_rows)

    except boto3.exceptions.Boto3Error as boto_err:
        logger.error(f"Boto3 error while processing {target_path}: {boto_err}")
        batch_errors.append({"Error": "Boto3 error", "Details": str(boto_err)})

    except Exception as e:
        logger.exception(f"Unexpected error during processing of {target_path}")
        batch_errors.append({"Error": "Unexpected error", "Details": str(e)})

    return {
        "Target": target_path,
        "SuccessCount": success_count,
        "Errors": batch_errors,
        "ErrorCount": len(batch_errors),
    }


def send_sqs_message(
    batch_id: str,
    timestamp: datetime,
    batch_number: int,
    principal_id: str,
    recipient_batch: List[Dict[str, Any]],
) -> Dict[Any, Any]:
    queue_name = os.getenv("EMAIL_BATCH_QUEUE_NAME")

    try:
        logger.info("processing sqs message...")

        message = {
            "BatchId": batch_id,
            "Recipients": recipient_batch,
            "Metadata": {"UploadedBy": principal_id, "Timestamp": timestamp},
        }

        logger.debug(f"sending batch {batch_number}...")

        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))

        logger.debug(
            f"batch {batch_number} successfully sent: {
                     json.dumps(message)}"
        )

        return response

    except boto3.exceptions.Boto3Error as boto_err:
        logger.error(f"Boto3 error when sending batch {batch_id}: {boto_err}")
        raise

    except Exception as e:
        logger.exception(f"Unexpected error in sending batch {batch_id}")
        raise


def send_email_to_admin(
    subject: str, body: str, csv_attachments: Dict[str, str] = {}
) -> None:
    msg = MIMEMultipart()
    msg["From"] = os.getenv("SES_NO_REPLY_SENDER")
    msg["To"] = os.getenv("SES_ADMIN_EMAIL")
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        for filename, content in csv_attachments.items():
            part = MIMEBase("application", "octet-stream")
            part.set_payload(content.encode("utf-8"))
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{filename.split('/')[-1]}"',
            )
            msg.attach(part)

        ses.send_raw_email(
            Source=os.getenv("SES_NO_REPLY_SENDER"),
            Destinations=os.getenv("SES_ADMIN_EMAIL").split(","),
            RawMessage={"Data": msg.as_string()},
        )
    except Exception as e:
        logger.error(f"Error constructing/sending delivery failure email to admin: {e}")

import json
import os
import logging
import io
import boto3
import boto3.exceptions
import botocore.exceptions
from datetime import datetime
from typing import Dict, Any, List, TypedDict, Literal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

logging.basicConfig(level=os.getenv("LOG_LEVEL"))
logger = logging.getLogger(__name__)

aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

sqs = boto3.client("sqs", aws_region)
s3 = boto3.client("s3", aws_region)
ses = boto3.client("sesv2", aws_region)


def move_s3_objects(bucket_list: Dict[str, List[Any]]) -> None:
    # copy the object from source to destination
    for bucket, objects in bucket_list.items():
        delete_list = []

        for object in objects:
            key = object.get("Key")
            object = key.split("/")[-1]
            try:
                s3.copy_object(
                    Bucket=bucket,
                    CopySource=f"{bucket}/{key}",
                    Key=f"{os.getenv("BATCH_INITIATION_ERROR_S3_PREFIX")}/{object}",
                )
                delete_list.append({"Key": key})
            except boto3.exceptions.Boto3Error as e:
                logger.exception(
                    f"Boto3 error copying s3 object - {f"{bucket}{key}"}: {e}"
                )
        else:
            # delete the object once copy is complete
            try:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_list})
            except boto3.exceptions.Boto3Error as e:
                logger.exception(
                    f"Boto3 error deleting s3 objects - {delete_list}: {e}"
                )


def get_s3_object(bucket_name: str, object_key: str) -> Dict[str, Any]:
    try:
        return s3.get_object(Bucket=bucket_name, Key=object_key)

    except boto3.exceptions.Boto3Error as e:
        logger.exception(f"Boto3 error at get_s3_object: {e}")


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

    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 error at send_sqs_message {batch_id}: {e}")
        raise


def send_email_to_admin(
    subject: str,
    body: str,
    csv_attachments: Dict[str, str] = {},
) -> None:
    try:
        msg = MIMEMultipart("mixed")
        msg["From"] = os.getenv("SES_NO_REPLY_SENDER")
        msg["To"] = os.getenv("SES_ADMIN_EMAIL")
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        # attach csv attachments to msg
        for filename, content in csv_attachments.items():
            part = MIMEBase("application", "octet-stream")
            part.set_payload(content.encode("utf-8"))
            encoders.encode_base64(part)
            file_name = filename.split("/")[-1]
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{file_name}"',
            )
            part.add_header("Content-ID", f"<{file_name}>")
            msg.attach(part)

        ses.send_email(
            FromEmailAddress=os.getenv("SES_NO_REPLY_SENDER"),
            Destination={
                "ToAddresses": os.getenv("SES_ADMIN_EMAIL").split(","),
            },
            Content={"Raw": {"Data": msg.as_string()}},
        )
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 error at send_email_to_admin: {e}")

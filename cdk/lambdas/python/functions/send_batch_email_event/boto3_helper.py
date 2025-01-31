import json
import os
import logging
import boto3
import boto3.exceptions
from botocore.exceptions import ClientError
from config import config
from collections import defaultdict
from typing import Dict, Any, List, Literal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)

aws_region = config.AWS_DEFAULT_REGION

sqs = boto3.client("sqs", aws_region)
s3 = boto3.client("s3", aws_region)
ses = boto3.client("sesv2", aws_region)
ddb = boto3.client("dynamodb", aws_region)


def get_s3_object(bucket_name: str, object_key: str) -> Dict[str, Any]:
    try:
        return s3.get_object(Bucket=bucket_name, Key=object_key)
    except s3.exceptions.NoSuchBucket:
        logger.exception(f"No bucket with name {bucket_name}")
        raise
    except ClientError as e:
        logger.exception(f"Unexpected Boto3 client error: {e}")
        raise
    except boto3.exceptions.Boto3Error as e:
        logger.exception(f"Boto3 library error: {e}")
        raise


def get_ddb_item(table_name: str, pk: str) -> Dict[Any, Any]:
    try:
        return ddb.get_item(TableName=table_name, Key={"template_key": {"S": pk}})
    except ddb.exceptions.ResourceNotFoundException:
        logger.exception(f"No item with key {pk} found")
        raise
    except ClientError as e:
        logger.exception(f"Unexpected Boto3 client error: {e}")
        raise
    except boto3.exceptions.Boto3Error as e:
        logger.exception(f"Boto3 library error: {e}")
        raise


def move_s3_objects(
    targets: List[Dict[Literal["From", "To"], Dict[Literal["Bucket", "Key"], str]]]
) -> None:
    # copy the object from source to destination
    delete_list = defaultdict(list)

    logger.info("copying targets...")
    for target in targets:
        source, destination = target["From"], target["To"]

        try:
            s3.copy_object(
                Bucket=destination["Bucket"],
                CopySource=source,
                Key=destination["Key"],
            )
            delete_list[source["Bucket"]].append({"Key": source["Key"]})
        except Exception as e:
            logger.exception(f"Error copying s3 object - {target}: {e}")

    logger.info("cleaning up source objects...")
    # delete the object once copy is complete
    for bucket, deleting_objects in delete_list.items():
        try:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": deleting_objects})
        except Exception as e:
            logger.exception(f"Error deleting s3 objects - {deleting_objects}: {e}")


def send_sqs_message(
    queue_name: str,
    message_body: Any,
) -> Dict[Any, Any]:
    try:
        logger.info("processing sqs message...")

        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        response = sqs.send_message(
            QueueUrl=queue_url, MessageBody=json.dumps(message_body)
        )

        logger.debug(f"message successfully sent to {queue_name}: {message_body}")

        return response

    except ClientError as e:
        logger.exception(f"Unexpected Boto3 client error: {e}")
        raise
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 error at send_sqs_message {message_body}: {e}")
        raise


def send_ses_email(
    send_from: str,
    send_to: str,
    subject: str,
    body: str,
    attachments: Dict[str, str] = {},
) -> None:
    try:
        msg = MIMEMultipart("mixed")
        msg["From"] = send_from
        msg["To"] = send_to
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))

        # attach csv attachments to msg
        for filename, content in attachments.items():
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
            FromEmailAddress=send_from,
            Destination={
                "ToAddresses": send_to.split(","),
            },
            Content={"Raw": {"Data": msg.as_string()}},
        )
    except ClientError as e:
        logger.exception(f"Unexpected Boto3 client error: {e}")
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 error at send_email_to_admin: {e}")

import json
import logging
import os
import boto3
import boto3.exceptions
from botocore.exceptions import ClientError
from collections import defaultdict, OrderedDict
from typing import Dict, Any, List, Literal, Optional, Mapping
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from mypy_boto3_sqs.client import SQSClient
from mypy_boto3_s3.client import S3Client
from mypy_boto3_sesv2.client import SESV2Client
from mypy_boto3_dynamodb.client import DynamoDBClient
from mypy_boto3_s3.type_defs import (
    CopySourceTypeDef,
    ObjectIdentifierTypeDef,
)
from botocore.response import StreamingBody
from mypy_boto3_sqs.type_defs import SendMessageResultTypeDef
from mypy_boto3_dynamodb.type_defs import (
    GetItemOutputTypeDef,
    UniversalAttributeValueTypeDef,
)

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

sqs: SQSClient = boto3.client("sqs", aws_region)
s3: S3Client = boto3.client("s3", aws_region)
ses: SESV2Client = boto3.client("sesv2", aws_region)
ddb: DynamoDBClient = boto3.client("dynamodb", aws_region)

EnabledEncodingTypes = Literal[
    "utf-8",
    "ascii",
]


# DynamoDB Operations
def get_ddb_item(table_name: str, pk: str) -> GetItemOutputTypeDef:
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


def put_ddb_item(table_name: str, item: Mapping[str, UniversalAttributeValueTypeDef]):
    try:
        return ddb.put_item(TableName=table_name, Item=item)
    except ClientError as e:
        logger.exception(f"Unexpected Boto3 client error: {e}")
        raise
    except boto3.exceptions.Boto3Error as e:
        logger.exception(f"Boto3 library error: {e}")
        raise


# S3 Operations
def get_s3_object(
    bucket_name: str,
    object_key: str,
    encoding_type: Optional[EnabledEncodingTypes] = "utf-8",
) -> str:
    try:
        res = s3.get_object(Bucket=bucket_name, Key=object_key)

        return res["Body"].read().decode(encoding_type)
    except s3.exceptions.NoSuchBucket:
        logger.exception(f"No bucket with name {bucket_name}")
        raise
    except s3.exceptions.NoSuchKey:
        logger.exception(f"No key with name {object_key}")
        raise
    except ClientError as e:
        logger.exception(f"Unexpected Boto3 client error: {e}")
        raise
    except boto3.exceptions.Boto3Error as e:
        logger.exception(f"Boto3 library error: {e}")
        raise


def move_s3_objects(
    targets: List[Dict[Literal["From", "To"], CopySourceTypeDef]]
) -> None:
    delete_list: Dict[str, List[ObjectIdentifierTypeDef]] = defaultdict(list)
    for target in targets:
        source = target["From"]
        destination = target["To"]

        try:
            logger.debug(f"copying {source} to {destination}")
            s3.copy_object(
                Bucket=destination["Bucket"],
                CopySource=source,
                Key=destination["Key"],
            )

            delete_list[source["Bucket"]].append({"Key": source["Key"]})
        except Exception as e:
            logger.exception(f"Error copying s3 object - {target}: {e}")

    logger.debug(f"cleaning up source objects... {delete_list}")
    # delete the object once copy is complete
    for bucket, deleting_objects in delete_list.items():
        try:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": deleting_objects})
        except Exception as e:
            logger.exception(f"Error deleting s3 objects - {deleting_objects}: {e}")


# SQS Operations
def send_sqs_message(
    queue_name: str,
    message_body: Any,
) -> SendMessageResultTypeDef:
    try:
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
    attachments: OrderedDict[str, str],
    body_type: Literal["html", "plain"] = "plain",
) -> None:
    """
    Sends an email using Amazon SES with optional attachments.

    This function creates a MIME multipart email message with the provided sender,
    recipient, subject, and body. The body can be formatted as either plain text or
    HTML (defaulting to plain text). Additionally, CSV or other file attachments
    can be included in the email.

    Parameters:
        send_from (str): The sender's email address as it will appear to recipients.
        send_to (str): The recipient's email address.
        subject (str): The subject line of the email.
        body (str): The email content.
        body_type (Literal["html", "plain"], optional): The format of the email body.
            Use "html" for HTML content or "plain" for plain text. Defaults to "plain".
        attachments (Dict[str, str], optional): A dictionary where the keys are
            filenames and the values are the paths to the files or the file content.
            Defaults to an empty dictionary (no attachments).

    Returns:
        None

    Raises:
        Exception: Propagates exceptions encountered while sending the email.
    """
    try:
        msg = MIMEMultipart("mixed")
        msg["From"] = send_from
        msg["To"] = send_to
        msg["Subject"] = subject
        msg.attach(MIMEText(body, body_type))

        logger.debug(f"attaching csvs... {attachments}")

        # attach csv attachments to MIMEMultipart
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

        logger.debug(f"attached all csv {msg}")

        logger.debug(f"sending ses email...")

        ses.send_email(
            FromEmailAddress=send_from,
            Destination={
                "ToAddresses": send_to.split(","),
            },
            Content={"Raw": {"Data": msg.as_string()}},
        )
        logger.debug(f"successfully sent all emails")

    except ClientError as e:
        logger.exception(f"Unexpected Boto3 client error: {e}")
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 error at send_email_to_admin: {e}")

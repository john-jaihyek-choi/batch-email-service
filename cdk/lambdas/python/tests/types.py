# stdlib
from typing import TypedDict, Callable, List, Dict, Any

# external libraries
from mypy_boto3_s3.literals import BucketLocationConstraintType


class S3EventRecordPayload(TypedDict):
    bucket_name: str
    object_key: str
    bucket_region: BucketLocationConstraintType
    event_name: str


GenerateMockS3LambdaEventFunction = Callable[
    [List[S3EventRecordPayload]], Dict[str, Any]
]

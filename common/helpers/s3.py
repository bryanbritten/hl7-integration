import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

MINIO_API_PORT = os.environ["MINIO_API_PORT"]
MINIO_ENDPOINT = f"minio:{MINIO_API_PORT}"
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BRONZE_BUCKET = "bronze"
MINIO_SILVER_BUCKET = "silver"
MINIO_GOLD_BUCKET = "gold"
MINIO_DEADLETTER_BUCKET = "deadletter"
FHIR_CONVERTER_API_VERSION = os.environ["FHIR_CONVERTER_API_VERSION"]
POLL_INTERVAL = 10  # seconds

s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)


def get_message_from_s3(bucket: str) -> tuple[str, bytes] | tuple[None, None]:
    """
    Retrieves a message from the specified S3 bucket.

    bucket: str - The name of the S3 bucket to retrieve the message from.
    Returns: bytes - The raw HL7 message retrieved from S3.
    """
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix="unprocessed/")
        if "Contents" not in response:
            return None, None

        key = response["Contents"][0].get("Key")
        if not key:
            return None, None

        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read()
        return key, content
    except Exception as e:
        print(f"Error retrieving message from S3: {e}")
        return None, None


def move_message_to_processed(bucket: str, source_key: str, destination_key: str) -> None:
    """
    Effectively changes the prefix of the key to move the message from "bronze/unprocessed"
    to "bronze/processed".
    """
    try:
        copy_source = {"Bucket": bucket, "Key": source_key}
        s3.copy_object(Bucket=bucket, CopySource=copy_source, Key=destination_key)
        s3.delete_object(Bucket=bucket, Key=source_key)
    except Exception as e:
        print(f"Error moving message: {e}")


def write_data_to_s3(
    bucket: str, key: str, body: bytes, content_type: str = "application/octet-stream"
) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )

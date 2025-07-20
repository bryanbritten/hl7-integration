import boto3
from botocore.config import Config
from validation import HL7Validator
import time

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
MINIO_BRONZE_BUCKET = "bronze"
MINIO_SILVER_BUCKET = "silver"
MINIO_DEADLETTER_BUCKET = "deadletter"
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
        response = s3.list_objects_v2(Bucket=bucket, Prefix="unprocessed/adt/a01/")
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


def move_message_to_processed(source_key: str, destination_key: str) -> None:
    """
    Effectively changes the prefix of the key to move the message from "bronze/unprocessed"
    to "bronze/processed".
    """
    try:
        copy_source = {"Bucket": MINIO_BRONZE_BUCKET, "Key": source_key}
        s3.copy_object(
            Bucket=MINIO_BRONZE_BUCKET, CopySource=copy_source, Key=destination_key
        )
        s3.delete_object(Bucket=MINIO_BRONZE_BUCKET, Key=source_key)
    except Exception as e:
        print(f"Error moving message: {e}")


def main() -> None:
    while True:
        key, message = get_message_from_s3(MINIO_BRONZE_BUCKET)
        if not message:
            time.sleep(POLL_INTERVAL)
            continue

        validator = HL7Validator(message)
        if not validator.message_is_valid():
            s3.put_object(
                Bucket=MINIO_DEADLETTER_BUCKET,
                Key=key,
                Body=message,
            )

            if not validator.msh_segment_is_valid():
                print(f"MSH segment is invalid in message {key}.")
            elif not validator.pid_segment_is_valid():
                print(f"PID segment is invalid in message {key}.")
            elif not validator.evn_segment_is_valid():
                print(f"EVN segment is invalid in message {key}.")
            elif not validator.pv1_segment_is_valid():
                print(f"PV1 segment is invalid in message {key}.")
            else:
                print(f"Message {key} failed to parse.")
        else:
            s3.put_object(
                Bucket=MINIO_SILVER_BUCKET,
                Key=key,
                Body=message,
            )
        move_message_to_processed(key, key.replace("unprocessed", "processed"))  # type: ignore


if __name__ == "__main__":
    main()

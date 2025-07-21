import json
import time
from typing import Any

import boto3
import requests
from botocore.config import Config

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
MINIO_SILVER_BUCKET = "silver"
MINIO_GOLD_BUCKET = "gold"
MINIO_DEADLETTER_BUCKET = "deadletter"
FHIR_CONVERTER_API_VERSION = "2024-05-01-preview"
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


def convert_hl7_to_fhir(
    message: bytes, message_type: str = "ADT_A01"
) -> dict[str, Any]:
    URL = f"http://fhir-converter:8080/convertToFhir?api-version={FHIR_CONVERTER_API_VERSION}"
    data = {
        "InputDataFormat": "Hl7v2",
        "RootTemplateName": message_type,
        "InputDataString": message.decode("utf-8"),
    }
    response = requests.post(URL, json=data)
    response.raise_for_status()
    return response.json()


def move_message_to_processed(source_key: str, destination_key: str) -> None:
    """
    Effectively changes the prefix of the key to move the message from "bronze/unprocessed"
    to "bronze/processed".
    """
    try:
        copy_source = {"Bucket": MINIO_SILVER_BUCKET, "Key": source_key}
        s3.copy_object(
            Bucket=MINIO_SILVER_BUCKET, CopySource=copy_source, Key=destination_key
        )
        s3.delete_object(Bucket=MINIO_SILVER_BUCKET, Key=source_key)
    except Exception as e:
        print(f"Error moving message: {e}")


def main() -> None:
    while True:
        key, message = get_message_from_s3(MINIO_SILVER_BUCKET)
        if not message:
            time.sleep(POLL_INTERVAL)
            continue

        fhir_data = convert_hl7_to_fhir(message=message, message_type="ADT_A01")
        if fhir_data is None:
            s3.put_object(
                Bucket=MINIO_DEADLETTER_BUCKET,
                Key=key.replace("unprocessed/adt/a01/", "adt/a01/messages/"),  # type: ignore
                Body=message,
            )
            issue = {
                "type": "conversion",
                "severity": 5,
                "message": "Failed to convert to FHIR.",
            }
            issue_json = json.dumps(issue)
            s3.put_object(
                Bucket=MINIO_DEADLETTER_BUCKET,
                Key=key.replace("unprocessed/adt/a01/", "adt/a01/issues/"),  # type: ignore
                Body=issue_json,
                ContentType="application/json",
            )
        else:
            s3.put_object(
                Bucket=MINIO_GOLD_BUCKET,
                Key=key.replace("unprocessed/", "").replace(".hl7", ".json"),  # type: ignore
                Body=json.dumps(fhir_data),
                ContentType="application/json",
            )
        move_message_to_processed(
            source_key=key,
            destination_key=key.replace("unprocessed", "processed"),  # type: ignore
        )


if __name__ == "__main__":
    main()

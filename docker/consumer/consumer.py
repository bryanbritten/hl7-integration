import asyncio
import boto3
from botocore.config import Config
import os

from asyncio import StreamReader, StreamWriter
from datetime import datetime, timezone

# In a production environment, these values would be in a .env file
PORT = 2575
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
MINIO_BUCKET = "bronze"

START = b"\x0b"
END = b"\x1c"
CR = b"\x0d"

# Initialize fake S3 bucket
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)


def build_ack() -> bytes:
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ack = f"MSH|^~\\&|RECEIVER|HOSP|SENDER|SYN|{now}||ACK^A01|MSGID1234|P|2.5\rMSA|AA|MSGID1234\r"
    return START + ack.encode("utf-8") + END + CR


def store_message(content: str) -> None:
    """
    Uploads an HL7 message to MinIO with a timestamped key.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    object_name = f"unprocessed/adt/a01/{timestamp}.hl7"
    try:
        s3.put_object(
            Bucket=MINIO_BUCKET, Key=object_name, Body=content.encode("utf-8")
        )
    # In production, specific Exceptions should be used for known or foreseeable issues
    except Exception as e:
        print(f"Error storing message: {e}")


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    try:
        data = await reader.readuntil(END + CR)
        raw_message = data.strip(END + CR).strip(START).decode()
        store_message(raw_message)
        writer.write(build_ack())
        await writer.drain()
        writer.close()
    # In production, specific Exceptions should be used for known or foreseeable issues
    except Exception as e:
        print(f"Error handling client: {e}")


async def main() -> None:
    server = await asyncio.start_server(handle_client, "0.0.0.0", PORT)
    async with server:
        print(f"Consumer service running on port {PORT}...")
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

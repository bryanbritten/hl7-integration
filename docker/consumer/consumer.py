import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from datetime import datetime, timezone

from prometheus_client import Counter, start_http_server
from s3_helpers import MINIO_BRONZE_BUCKET, write_data_to_s3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

START = b"\x0b"
END = b"\x1c"
CR = b"\x0d"
PORT = 2575

messages_received_total = Counter(
    "messages_received_total",
    "Total number of HL7 messages received by the Consumer service.",
    ["message_type"],
)


def build_ack() -> bytes:
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ack = f"MSH|^~\\&|RECEIVER|HOSP|SENDER|SYN|{now}||ACK^A01|MSGID1234|P|2.5\rMSA|AA|MSGID1234\r"
    return START + ack.encode("utf-8") + END + CR


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    try:
        data = await reader.readuntil(END + CR)
        message = data.strip(END + CR).strip(START)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        key = f"unprocessed/adt/a01/{timestamp}.hl7"
        write_data_to_s3(
            bucket=MINIO_BRONZE_BUCKET,
            key=key,
            body=message,
        )
        logger.info("Message received")
        messages_received_total.labels(message_type="ADT_A01").inc()

        # acknowledge receipt
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
    start_http_server(8000)
    asyncio.run(main())

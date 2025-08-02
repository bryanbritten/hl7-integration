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


def get_msh_segment(message: bytes) -> str:
    segments = message.split(CR)
    return str(segments[0])


def build_ack(
    msh_segment: str, acknowledgement_code: str, message_control_id: str
) -> bytes:
    separator = msh_segment[3]
    msa_segment = separator.join(
        [
            "MSA",
            acknowledgement_code,
            message_control_id,
        ]
    ).encode("utf-8")

    return START + msh_segment.encode("utf-8") + CR + msa_segment + END + CR


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    try:
        data = await reader.readuntil(END + CR)
        message = data.strip(END + CR).strip(START)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        logger.info("Message received")

        msh_segment = get_msh_segment(message)
        separator = msh_segment[3]
        fields = msh_segment.split(separator)
        message_type = fields[8]
        message_control_id = fields[9]
        messages_received_total.labels(message_type=message_type).inc()

        key = f"unprocessed/{message_type[:3]}/{message_type[4:]}/{timestamp}.hl7"
        write_data_to_s3(
            bucket=MINIO_BRONZE_BUCKET,
            key=key,
            body=message,
        )

        ack = build_ack(
            msh_segment=msh_segment,
            acknowledgement_code="AA",
            message_control_id=message_control_id,
        )
        writer.write(ack)
        await writer.drain()
        writer.close()
    # In production, specific Exceptions should be used for known or foreseeable issues
    # TODO: identify how to send ACK on error
    except Exception as e:
        logger.exception(f"Error handling client: {e}")


async def main() -> None:
    server = await asyncio.start_server(handle_client, "0.0.0.0", PORT)
    async with server:
        logger.info(f"Consumer service running on port {PORT}...")
        await server.serve_forever()


if __name__ == "__main__":
    start_http_server(8000)
    asyncio.run(main())

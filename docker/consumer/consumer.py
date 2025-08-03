import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from datetime import datetime, timezone

from hl7_helpers import get_msh_segment
from prometheus_client import Counter, start_http_server
from s3_helpers import MINIO_BRONZE_BUCKET, MINIO_DEADLETTER_BUCKET, write_data_to_s3

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


def build_ack(
    msh_segment: str,
    separator: str,
    acknowledgement_code: str,
    message_control_id: str,
) -> bytes:
    msa_segment = separator.join(
        [
            "MSA",
            acknowledgement_code,
            message_control_id,
        ]
    ).encode("utf-8")

    return START + msh_segment.encode("utf-8") + CR + msa_segment + END + CR


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    # instantiate variables to be used in exception handling
    timestamp = None
    message = None
    msh_segment = None
    message_control_id = None

    try:
        data = await reader.readuntil(END + CR)
        message = data.strip(END + CR).strip(START)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        logger.info("Message received")

        msh_segment = get_msh_segment(message)
        separator = msh_segment.msh_1.to_er7()
        message_type = msh_segment.msh_9.msh_9_1.to_er7()
        trigger_event = msh_segment.msh_9.msh_9_2.to_er7()
        message_control_id = msh_segment.msh_10.to_er7()
        messages_received_total.labels(message_type=message_type).inc()

        key = f"unprocessed/{message_type}/{trigger_event}/{timestamp}.hl7"
        write_data_to_s3(
            bucket=MINIO_BRONZE_BUCKET,
            key=key,
            body=message,
        )

        ack = build_ack(
            msh_segment=msh_segment.to_er7(),
            separator=separator,
            acknowledgement_code="AA",
            message_control_id=message_control_id,
        )
        writer.write(ack)
        await writer.drain()
        writer.close()
    # In production, specific Exceptions should be used for known or foreseeable issues
    except Exception as e:
        # if there is no message then there was an error receiving the message; no need for ACK
        if message is None:
            logger.exception(f"Failed to ingest message: {str(e)}")
        # if the message_control_id is None then the message either failed to parse
        # or had a malformed MSH header. Either way, no way to automatically send ACK.
        elif message_control_id is None:
            if timestamp is None:
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
            write_data_to_s3(
                bucket=MINIO_DEADLETTER_BUCKET,
                key=f"unparsed/{timestamp}.hl7",
                body=message,
            )
        elif msh_segment is not None:
            logger.exception(f"Error handling client: {str(e)}")
            ack = build_ack(
                msh_segment=msh_segment.to_er7(),
                separator=msh_segment.msh_1.to_er7(),
                acknowledgement_code="AE",
                message_control_id=message_control_id,
            )
            writer.write(ack)
            await writer.drain()
            writer.close()
        else:
            logger.exception(f"Unexpected error: {str(e)}")


async def main() -> None:
    server = await asyncio.start_server(handle_client, "0.0.0.0", PORT)
    async with server:
        logger.info(f"Consumer service running on port {PORT}...")
        await server.serve_forever()


if __name__ == "__main__":
    start_http_server(8000)
    asyncio.run(main())

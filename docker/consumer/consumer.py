import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from datetime import datetime, timezone

from hl7_helpers import get_msh_segment
from hl7apy.exceptions import ParserError
from metrics import (
    messages_received_total,
)
from prometheus_client import start_http_server
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


def manually_extract_msh_segment(message: str) -> str:
    segments = message.split("\r")
    msh_candidates = [segment for segment in segments if segment.startswith("MSH")]
    if not any(msh_candidates):
        raise ValueError(f"No MSH segment present: {segments}")
    return msh_candidates[0]


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

        try:
            msh_segment = get_msh_segment(message)
            separator = msh_segment.msh_1.to_er7()
            message_type = msh_segment.msh_9.msh_9_1.to_er7()
            trigger_event = msh_segment.msh_9.msh_9_2.to_er7()
            message_control_id = msh_segment.msh_10.to_er7()
        except ParserError:
            logger.warning(
                "Failed to automatically extract MSH segment, so doing it manually"
            )
            msh_segment = manually_extract_msh_segment(message.decode("utf-8"))
            separator = msh_segment[3]
            fields = msh_segment.split(separator)
            message_type, trigger_event, _ = fields[8].split("^")
            message_control_id = fields[9]
        if not isinstance(msh_segment, str):
            msh_segment = msh_segment.to_er7()

        messages_received_total.labels(
            message_type=f"{message_type}_{trigger_event}"
        ).inc()

        key = f"unprocessed/{message_type}/{trigger_event}/{timestamp}.hl7"
        write_data_to_s3(
            bucket=MINIO_BRONZE_BUCKET,
            key=key,
            body=message,
        )

        ack = build_ack(
            msh_segment=msh_segment,
            separator=separator,
            acknowledgement_code="AA",
            message_control_id=message_control_id,
        )
        writer.write(ack)
        await writer.drain()
        writer.close()
    # In production, specific Exceptions should be used for known or foreseeable issues
    except Exception as e:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")

        # if there is no message then there was an error receiving the message; no need for ACK
        if message is None:
            logger.exception(f"Failed to ingest message: {str(e)}")
        # if the message_control_id is None then the message either failed to parse
        # or had a malformed MSH header. Either way, no way to automatically send ACK.
        elif message_control_id is None:
            logger.exception(f"Failed to parse the MSH header: {str(e)}")
        # if the message was received and the MSH header was parsed, then something
        # unidentified occurred.
        elif msh_segment is not None:
            logger.exception(f"Error handling client: {str(e)}")
            ack = build_ack(
                msh_segment=msh_segment,
                separator=separator,  # type: ignore
                acknowledgement_code="AE",
                message_control_id=message_control_id,
            )
            writer.write(ack)
            await writer.drain()
            writer.close()
        else:
            logger.exception(f"Unexpected error: {str(e)}")
        write_data_to_s3(
            bucket=MINIO_DEADLETTER_BUCKET,
            key=f"unparsed/{timestamp}.hl7",
            body=message,
        )


async def main() -> None:
    server = await asyncio.start_server(handle_client, "0.0.0.0", PORT)
    async with server:
        logger.info(f"Consumer service running on port {PORT}...")
        await server.serve_forever()


if __name__ == "__main__":
    start_http_server(8000)
    asyncio.run(main())

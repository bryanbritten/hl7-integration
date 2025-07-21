import asyncio
from asyncio import StreamReader, StreamWriter
from datetime import datetime, timezone

from s3_helpers import MINIO_BRONZE_BUCKET, write_data_to_s3

START = b"\x0b"
END = b"\x1c"
CR = b"\x0d"
PORT = 2575


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

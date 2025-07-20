import asyncio
import os

from asyncio import StreamReader, StreamWriter
from datetime import datetime, timezone

PORT = 2575
OUTPUT_DIR = "/messages"

START = b"\x0b"
END = b"\x1c"
CR = b"\x0d"


def build_ack() -> bytes:
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ack = f"MSH|^~\\&|RECEIVER|HOSP|SENDER|SYN|{now}||ACK^A01|MSGID1234|P|2.5\rMSA|AA|MSGID1234\r"
    return START + ack.encode("utf-8") + END + CR


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    try:
        data = await reader.readuntil(END + CR)
        raw_message = data.strip(END + CR).strip(START).decode()
        timestamp = datetime.now(timezone.utc).isoformat()
        file_path = os.path.join(OUTPUT_DIR, f"msg_{timestamp}.hl7")
        with open(file_path, "w") as f:
            f.write(raw_message)
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

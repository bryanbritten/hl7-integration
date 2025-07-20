import random
import socket
import time

from hl7_generators import (
    build_msh_segment,
    build_pid_segment,
    build_pv1_segment,
    build_evn_segment,
)

CONSUMER_HOST = "consumer"
CONSUMER_PORT = 2575

START = b"\x0b"  # VT
END = b"\x1c"  # FS
CR = b"\x0d"  # CR


def build_adt_message() -> bytes:
    """
    Builds a complete ADT message with MSH, PID, PV1, and EVN segments.

    Returns: bytes - The complete ADT message encoded in UTF-8.
    """

    msh_segment = build_msh_segment(message_type="ADT^A01")
    pid_segment = build_pid_segment()
    pv1_segment = build_pv1_segment()
    evn_segment = build_evn_segment("A01")

    return msh_segment + CR + pid_segment + CR + pv1_segment + CR + evn_segment


def send_message(message: bytes) -> bytes | None:
    """
    Sends the HL7 message to the consumer service.

    message: bytes - The HL7 message to send.
    """

    try:
        with socket.create_connection(
            (CONSUMER_HOST, CONSUMER_PORT), timeout=10
        ) as sock:
            sock.sendall(START + message + END + CR)
            ack = sock.recv(4096)
            return ack
    # In a production environment, specific exceptions should be caught
    except Exception as e:
        print(f"Error sending message: {e}")
        return None


def main():
    while True:
        adt_message = build_adt_message()
        ack = send_message(adt_message)

        if ack:
            print(f"Received ACK: {ack.decode('utf-8')}")
        else:
            print("No ACK received or an error occurred.")

        time.sleep(random.uniform(1, 5))


if __name__ == "__main__":
    main()

import json
import logging
import time
from typing import Any

import requests
from prometheus_client import Counter, start_http_server
from s3_helpers import (
    FHIR_CONVERTER_API_VERSION,
    MINIO_DEADLETTER_BUCKET,
    MINIO_GOLD_BUCKET,
    MINIO_SILVER_BUCKET,
    POLL_INTERVAL,
    get_message_from_s3,
    move_message_to_processed,
    write_data_to_s3,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

messages_fhir_conversion_attempts = Counter(
    "messages_fhir_conversion_attempts",
    "Total number of HL7 messages sent to the FHIR Converter API.",
    ["message_type"],
)
messages_fhir_conversion_successes = Counter(
    "messages_fhir_conversion_successes",
    "Total number of HL7 messages successfully converted to the FHIR format.",
    ["message_type"],
)


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


def main() -> None:
    while True:
        key, message = get_message_from_s3(MINIO_SILVER_BUCKET)
        if not message:
            time.sleep(POLL_INTERVAL)
            continue

        messages_fhir_conversion_attempts.labels(message_type="ADT_A01").inc()
        fhir_data = convert_hl7_to_fhir(message=message, message_type="ADT_A01")
        if fhir_data is None:
            write_data_to_s3(
                bucket=MINIO_DEADLETTER_BUCKET,
                key=key.replace("unprocessed/adt/a01/", "adt/a01/messages"),  # type: ignore
                body=message,
            )

            issue = {
                "type": "conversion",
                "severity": 5,
                "message": "Failed to convert to FHIR.",
            }
            write_data_to_s3(
                bucket=MINIO_DEADLETTER_BUCKET,
                key=key.replace("unprocessed/adt/a01/", "adt/a01/issues/"),  # type: ignore
                body=json.dumps(issue).encode("utf-8"),
                content_type="application/json",
            )
            logger.error("Failed to convert message to FHIR")
        else:
            write_data_to_s3(
                bucket=MINIO_GOLD_BUCKET,
                key=key.replace("unprocessed/", "").replace(".hl7", ".json"),  # type: ignore
                body=json.dumps(fhir_data).encode("utf-8"),
                content_type="application/json",
            )
            logger.info("Successfully converted HL7 message to FHIR")
            messages_fhir_conversion_successes.labels(message_type="ADT_A01").inc()
        move_message_to_processed(
            bucket=MINIO_SILVER_BUCKET,
            source_key=key,
            destination_key=key.replace("unprocessed", "processed"),  # type: ignore
        )


if __name__ == "__main__":
    start_http_server(8000)
    main()

import requests
from requests import Response

from common.helpers.kafka import to_header, write_to_topic


def convert_hl7_to_fhir(message: bytes, message_type: str, fhir_url: str) -> Response:
    URL = fhir_url
    data = {
        "InputDataFormat": "Hl7v2",
        "RootTemplateName": message_type,
        "InputDataString": message.decode("utf-8"),
    }
    response = requests.post(URL, json=data)
    return response


def handle_error(
    e: Exception,
    message: bytes,
    message_type: str,
    dlq_topic: str,
) -> None:
    headers = [
        to_header("error.stage", "qa"),
        to_header("error.message", f"Unexpected error occurred: {e}"),
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", "hl7.transformers"),
    ]
    write_to_topic(message, dlq_topic, headers)


def handle_failures(*args) -> None:
    pass


def handle_success(message: bytes, message_type: str, write_bucket: str) -> None:
    pass


def process_message(
    message: bytes,
    message_type: str,
    fhir_url: str,
    silver_bucket_name: str,
    dlq_topic: str,
) -> None:
    try:
        fhir_api_response = convert_hl7_to_fhir(message, message_type, fhir_url)
        fhir_api_response.raise_for_status()

        results = fhir_api_response.json()

        handle_success(message, message_type, silver_bucket_name)
    except Exception as e:
        handle_error(
            e=e,
            message=message,
            message_type=message_type,
            dlq_topic=dlq_topic,
        )
        return

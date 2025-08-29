from common.helpers.fhir import convert_hl7_to_fhir
from common.helpers.kafka import to_header, write_to_topic


def handle_error(
    e: Exception,
    message: bytes,
    message_type: str,
    dlq_topic: str,
    group_id: str,
) -> None:
    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", group_id),
        to_header("error.stage", "transformation"),
        to_header("error.type", "error"),
        to_header("error.message", f"Unexpected error occurred: {e}"),
    ]
    write_to_topic(message, dlq_topic, headers)


def handle_failures(
    failures: ..., message: bytes, message_type: str, dlq_topic: str, group_id: str
) -> None:
    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", group_id),
        to_header("error.stage", "transformation"),
        to_header("error.type", "failure"),
        to_header("error.message", failures),
    ]
    write_to_topic(message, dlq_topic, headers)


def handle_success(
    message: bytes, message_type: str, s3key: str, write_bucket: str, group_id: str
) -> None:
    pass


def process_message(
    message: bytes,
    message_type: str,
    fhir_url: str,
    s3key: str,
    silver_bucket_name: str,
    dlq_topic: str,
    group_id: str,
) -> None:
    try:
        fhir_api_response = convert_hl7_to_fhir(message, message_type, fhir_url)
        fhir_api_response.raise_for_status()

        results = fhir_api_response.json().get("result")
        if not results:
            handle_failures(..., message, message_type, dlq_topic, group_id)
            return

        handle_success(message, message_type, s3key, silver_bucket_name, group_id)
    except Exception as e:
        handle_error(e, message, message_type, dlq_topic, group_id)
        return

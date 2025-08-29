import json

from requests.exceptions import HTTPError

from common.helpers.fhir import convert_hl7_to_fhir
from common.helpers.kafka import to_header, write_to_topic
from common.helpers.s3 import write_data_to_s3
from common.metrics.counters import fhir_conversion_attempted_total
from common.metrics.labels import (
    REASON_EMPTY_RESULTS,
    REASON_HTTP_ERROR,
    REASON_UNKNOWN,
)
from common.metrics.utils import record_transformation_failures


def handle_error(
    e: Exception,
    message: bytes,
    message_type: str,
    dlq_topic: str,
    group_id: str,
) -> None:
    if isinstance(e, HTTPError):
        failures = {REASON_HTTP_ERROR: True}
    else:
        failures = {REASON_UNKNOWN: True}

    error_message = list(failures.keys())[0]

    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", group_id),
        to_header("error.stage", "transformation"),
        to_header("error.type", "error"),
        to_header("error.message", f"{error_message}: {e}"),
    ]
    write_to_topic(message, dlq_topic, headers)

    record_transformation_failures(message_type, failures)


def handle_failures(
    failures: dict[str, bool], message: bytes, message_type: str, dlq_topic: str, group_id: str
) -> None:
    headers = [
        to_header("hl7.message.type", message_type),
        to_header("consumer.group.id", group_id),
        to_header("error.stage", "transformation"),
        to_header("error.type", "failure"),
        to_header("error.message", json.dumps(failures)),
    ]
    write_to_topic(message, dlq_topic, headers)

    record_transformation_failures(message_type, failures)


def handle_success(message: bytes, s3key: str, write_bucket: str) -> None:
    write_data_to_s3(
        bucket=write_bucket,
        key=s3key,
        body=message,
    )


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
        fhir_conversion_attempted_total.labels(message_type).inc()
        fhir_api_response = convert_hl7_to_fhir(message, message_type, fhir_url)
        fhir_api_response.raise_for_status()

        results = fhir_api_response.json().get("result")
        if not results:
            handle_failures(
                {REASON_EMPTY_RESULTS: True}, message, message_type, dlq_topic, group_id
            )
            return

        handle_success(message, s3key, silver_bucket_name)
    except Exception as e:
        handle_error(e, message, message_type, dlq_topic, group_id)
        return

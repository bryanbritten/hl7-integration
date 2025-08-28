from typing import Optional

from common.metrics.counters import validation_error_total, validation_failed_total

REASON_MISSING_MSH10 = "Missing MSH-10 Field"
REASON_MISSING_SEGMENTS = "Missing Required Segments"
REASON_INVALID_SEGMENTS = "Contains Invalid Segments"
REASON_INVALID_CARDINALITY = "Invalid Segment Cardinality"
REASON_PARSING_ERROR = "Message Failed Parsing"
REASON_UNKNOWN = "Unexpected Error Occurred"


def record_validation_failures(
    message_type: str,
    message_control_id: Optional[str] = None,
    failures: dict[str, str] = {},
) -> None:
    reasons = []
    if message_control_id is None:
        reasons.append(REASON_MISSING_MSH10)
    if failures.get(REASON_MISSING_SEGMENTS):
        reasons.append(REASON_MISSING_SEGMENTS)
    if failures.get(REASON_INVALID_SEGMENTS):
        reasons.append(REASON_INVALID_SEGMENTS)
    if failures.get(REASON_INVALID_CARDINALITY):
        reasons.append(REASON_INVALID_CARDINALITY)

    for reason in reasons:
        validation_error_total.labels(reason, "ingest", message_type).inc()

    if reasons:
        validation_failed_total.labels(message_type).inc()

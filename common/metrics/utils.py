from typing import Optional

from common.metrics.counters import validation_error_total, validation_failed_total
from common.metrics.labels import REASON_MISSING_MSH10


def record_validation_failures(
    message_type: str,
    message_control_id: Optional[str] = None,
    failures: dict[str, bool] = {},
) -> None:
    reasons = [k for k, v in failures.items() if v]
    if message_control_id is None:
        reasons.append(REASON_MISSING_MSH10)

    for reason in reasons:
        validation_error_total.labels(reason, "ingest", message_type).inc()

    if reasons:
        validation_failed_total.labels(message_type).inc()

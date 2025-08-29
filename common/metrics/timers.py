from prometheus_client import Histogram

DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10)

hl7_stage_duration_seconds = Histogram(
    "hl7_stage_duration_seconds",
    "Time spent per HL7 processing stage",
    ["stage", "message_type"],
    buckets=DURATION_BUCKETS,
)

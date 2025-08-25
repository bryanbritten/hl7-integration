from prometheus_client import Counter

messages_sent_total = Counter(
    "messages_sent_total", "Total number of HL7 messages sent", ["message_type"]
)

messages_received_total = Counter(
    "messages_received_total",
    "Total number of HL7 messages received by the Consumer service.",
    ["message_type"],
)

hl7_acks_total = Counter(
    "hl7_acks_total",
    "Total number of HL7 ACK messages sent by the Consumer service.",
    ["status"],
)

message_failures_total = Counter(
    "message_failures_total",
    "Total number of HL7 messages that failed validation, or data quality checks",
    ["type", "details", "stage", "message_type"],
)

messages_passed_total = Counter(
    "messages_passed_total",
    "Total number of recieved HL7 messages that passed validation and all data quality checks",
    ["message_type"],
)

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

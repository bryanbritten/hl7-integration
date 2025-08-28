from prometheus_client import Counter

messages_sent_total = Counter(
    "messages_sent_total", "Total number of HL7 messages sent", ["message_type"]
)

messages_received_total = Counter(
    "messages_received_total",
    "Total number of HL7 messages received/processed by the Consumer service.",
    ["message_type"],
)

messages_accepted_total = Counter(
    "messages_accepted_total",
    "Total number of HL7 messages that cleared the ingestion process in the Consumer service.",
    ["message_type"],
)

hl7_acks_total = Counter(
    "hl7_acks_total",
    "Total number of HL7 ACK messages sent by the Consumer service.",
    ["status"],
)

validation_failed_total = Counter(
    "validation_failed_total",
    "Total number of HL7 messages that failed the ingestion process.",
    ["message_type"],
)

validation_error_total = Counter(
    "validation_error_total",
    "Count of valiation errors by reason (buckets are not mutually exclusive)",
    ["details", "stage", "message_type"],
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

from prometheus_client import Counter

messages_received_total = Counter(
    "messages_received_total",
    "Total number of HL7 messages received by the Consumer service.",
    ["message_type"],
)

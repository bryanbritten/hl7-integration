import os

from confluent_kafka import Producer

producer = Producer(
    {
        "bootstrap.servers": os.environ["KAFKA_BROKERS"],
        "acks": 1,
    }
)


def send_to_topic(message: bytes, topic: str) -> None:
    producer.produce(
        topic=topic,
        value=message,
    )
    producer.flush()

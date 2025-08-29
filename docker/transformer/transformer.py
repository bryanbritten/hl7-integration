import logging
import os
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition
from dotenv import load_dotenv
from prometheus_client import start_http_server

from utils import process_message

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

FHIR_CONVERTER_API_VERSION = os.environ["FHIR_CONVERTER_API_VERSION"]
FHIR_CONVERTER_BASE_URL = os.environ["FHIR_CONVERTER_BASE_URL"]
FHIR_URL = f"{FHIR_CONVERTER_BASE_URL}?api-version={FHIR_CONVERTER_API_VERSION}"
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
GROUP_ID = os.environ["TRANSFORMER_GROUP_ID"]
READ_TOPIC = os.environ["TRANSFORMER_READ_TOPIC"]
DLQ_TOPIC = os.environ["DLQ_TOPIC"]
WRITE_BUCKET = "silver"


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([READ_TOPIC])

    while True:
        msg = consumer.poll(timeout=10.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logger.error(f"Kafka consumer error: {msg.error()}")
                continue

        message = msg.value()
        message_type = next(
            (v.decode("utf-8") for k, v in msg.headers() if k == "hl7.message.type"),
            "",
        )
        s3key = next((v.decode("utf-8") for k, v in msg.headers() if k == "hl7.message.s3key"), "")

        try:
            process_message(message, message_type, FHIR_URL, s3key, WRITE_BUCKET, DLQ_TOPIC)
            consumer.commit(message=msg)
        except Exception as e:
            logger.exception(f"Processing failed. Not committing offset. Details: {e}")

            # "rewind" the partition so that it sticks on the broken message
            # this will cause this consumer to freeze on that message causing
            # an alert so that it can be fixed.
            # TODO: Determine what metric makes sense here
            tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
            consumer.seek(tp)
            time.sleep(1.0)
            continue


if __name__ == "__main__":
    start_http_server(8000)
    logger.info(
        f"Transformer service starting. Brokers: {KAFKA_BROKERS} Topics: R:{READ_TOPIC} Bucket: {WRITE_BUCKET}"
    )
    main()

FROM python:3.11-slim

WORKDIR /producer

COPY docker/producer /producer
COPY common /producer/common

RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir confluent-kafka faker hl7apy prometheus-client python-dotenv

CMD ["python", "producer.py"]
FROM python:3.11-slim

WORKDIR /producer

COPY docker/producer /producer
COPY common/scripts/hl7_helpers.py /producer

RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir confluent-kafka faker hl7apy prometheus-client python-dotenv

EXPOSE 8000

CMD ["python", "producer.py"]
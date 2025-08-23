FROM python:3.11-slim

WORKDIR /producer

COPY docker/producer /producer
COPY common/scripts/hl7_helpers.py /producer

RUN pip install --no-cache-dir faker hl7apy prometheus-client python-dotenv

EXPOSE 8000

CMD ["python", "producer.py"]
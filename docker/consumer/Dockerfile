FROM python:3.11-slim

WORKDIR /consumer

COPY docker/consumer/ /consumer
COPY common/scripts/ /consumer

RUN mkdir /messages
RUN pip install --no-cache-dir boto3 hl7apy prometheus-client python-dotenv

EXPOSE 2575 8000

CMD ["python", "consumer.py"]
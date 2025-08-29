FROM python:3.11-slim

WORKDIR /transformer

COPY docker/transformer/ /transformer
COPY common /transformer/common

RUN pip install --no-cache-dir boto3 confluent-kafka hl7apy requests prometheus-client python-dotenv

EXPOSE 8000

CMD [ "python", "transformer.py" ]
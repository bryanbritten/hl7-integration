FROM python:3.11-slim

WORKDIR /qa

COPY docker/qa /qa
COPY common /qa/common

RUN pip install --no-cache-dir boto3 confluent-kafka hl7apy prometheus-client python-dotenv

EXPOSE 8000

CMD [ "python", "qa.py" ]
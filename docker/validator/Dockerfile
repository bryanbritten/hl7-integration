FROM python:3.11-slim

WORKDIR /validator

COPY docker/validator/ /validator
COPY common/scripts/ /validator

RUN pip install --no-cache-dir boto3 hl7apy prometheus-client python-dotenv

EXPOSE 8000

CMD [ "python", "validator.py" ]
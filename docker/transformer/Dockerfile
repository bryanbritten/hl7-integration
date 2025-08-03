FROM python:3.11-slim
WORKDIR /transformer
COPY docker/transformer/transformer.py /transformer/transformer.py
COPY common/scripts/s3_helpers.py /transformer/s3_helpers.py
COPY common/scripts/hl7_helpers.py /transformer/hl7_helpers.py
RUN pip install --no-cache-dir prometheus-client boto3 requests
EXPOSE 8000
CMD [ "python", "transformer.py" ]
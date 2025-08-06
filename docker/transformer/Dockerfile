FROM python:3.11-slim
WORKDIR /transformer
COPY docker/transformer/transformer.py /transformer/transformer.py
COPY docker/transformer/metrics.py /transformer/metrics.py
COPY common/scripts/s3_helpers.py /transformer/s3_helpers.py
COPY common/scripts/hl7_helpers.py /transformer/hl7_helpers.py
RUN pip install --no-cache-dir boto3 hl7apy requests prometheus-client
EXPOSE 8000
CMD [ "python", "transformer.py" ]
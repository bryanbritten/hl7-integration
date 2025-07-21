FROM python:3.11-slim
WORKDIR /transformer
COPY docker/transformer/validation.py /transformer/validation.py
COPY docker/transformer/quality_assurance.py /transformer/quality_assurance.py
COPY docker/transformer/entrypoint.py /transformer/entrypoint.py
COPY common/s3_helpers.py /transformer/s3_helpers.py
RUN pip install --no-cache-dir hl7apy boto3
CMD [ "python", "entrypoint.py" ]
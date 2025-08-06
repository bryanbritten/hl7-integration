FROM python:3.11-slim
WORKDIR /validator
COPY docker/validator/validation.py /validator/validation.py
COPY docker/validator/quality_assurance.py /validator/quality_assurance.py
COPY docker/validator/validator.py /validator/validator.py
COPY docker/validator/metrics.py /validator/metrics.py
COPY common/scripts/s3_helpers.py /validator/s3_helpers.py
COPY common/scripts/hl7_helpers.py /validator/hl7_helpers.py
RUN pip install --no-cache-dir boto3 hl7apy prometheus-client
EXPOSE 8000
CMD [ "python", "validator.py" ]
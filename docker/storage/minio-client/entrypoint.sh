#!/bin/sh
set -e

until mc alias set local "http://minio:9000" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  echo "Waiting for MinIO..."
  sleep 1
done

echo "Successfully connected to MinIO..."

for b in bronze silver gold deadletter; do
  mc mb --ignore-existing "local/$b" || true
  mc anonymous set public "local/$b" || true
done

exit 0

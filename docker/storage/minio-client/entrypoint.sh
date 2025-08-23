#!/bin/sh

sleep 5

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing local/bronze
mc anonymous set public local/bronze
mc mb --ignore-existing local/silver
mc anonymous set public local/silver
mc mb --ignore-existing local/gold
mc anonymous set public local/gold
mc mb --ignore-existing local/deadletter
mc anonymous set public local/deadletter

# keep the container alive
tail -f /dev/null

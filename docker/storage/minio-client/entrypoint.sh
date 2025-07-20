#!/bin/sh

sleep 5

mc alias set local http://minio:9000 admin password123
mc mb --ignore-existing local/bronze
mc anonymous set public local/bronze
mc mb --ignore-existing local/silver
mc anonymous set public local/silver
mc mb --ignore-existing local/gold
mc anonymous set public local/gold

# keep the container alive
tail -f /dev/null

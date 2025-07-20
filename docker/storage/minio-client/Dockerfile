FROM minio/mc:RELEASE.2025-07-16T15-35-03Z-cpuv1

COPY entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
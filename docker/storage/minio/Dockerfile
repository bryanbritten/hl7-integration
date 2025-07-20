FROM minio/minio:RELEASE.2025-07-18T21-56-31Z-cpuv1

ENV MINIO_ROOT_USER=admin
ENV MINIO_ROOT_PASSWORD=password123
RUN mkdir -p /data
EXPOSE 9000
EXPOSE 9001

# Start MinIO server with admin console
ENTRYPOINT ["minio", "server", "/data", "--console-address", ":9001"]
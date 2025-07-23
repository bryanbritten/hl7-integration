FROM grafana/grafana:12.0.2-security-01-ubuntu
ENV GF_SECURITY_ADMIN_USER=admin
ENV GF_SECURITY_ADMIN_PASSWORD=admin
ENV GF_USERS_ALLOW_SIGN_UP=false
COPY dashboards /etc/grafana/dashboards
COPY provisioning /etc/grafana/provisioning
EXPOSE 3000

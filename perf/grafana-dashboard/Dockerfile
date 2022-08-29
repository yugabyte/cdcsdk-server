FROM grafana/grafana:latest
COPY dashboard.yml /etc/grafana/provisioning/dashboards
COPY datasource.yml /etc/grafana/provisioning/datasources
COPY sample-dashboard.json /var/lib/grafana/dashboards/sample-dashboard.json
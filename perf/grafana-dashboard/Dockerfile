FROM grafana/grafana:latest
COPY dashboard.yml /etc/grafana/provisioning/dashboards
COPY datasource.yml /etc/grafana/provisioning/datasources
COPY streaming-dashboard.json /var/lib/grafana/dashboards/streaming-dashboard.json
COPY snapshot-dashboard.json /var/lib/grafana/dashboards/snapshot-dashboard.json
FROM quay.io/yugabyte/debezium-connector:latest
RUN mkdir /kafka/etc && cd /kafka/etc && curl -so jmx_prometheus_javaagent-0.9.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.9/jmx_prometheus_javaagent-0.9.jar
COPY kafka-connect.yml /etc/jmx-exporter/kafka-connect.yml
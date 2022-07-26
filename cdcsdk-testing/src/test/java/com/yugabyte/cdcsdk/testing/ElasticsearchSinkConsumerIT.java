package com.yugabyte.cdcsdk.testing;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Iterator;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;

public class ElasticsearchSinkConsumerIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSinkConsumerIT.class);
    private static final String ELASTIC_SEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:7.3.0";
    private static final String KAFKA_CONNECT_IMAGE = "quay.io/yugabyte/connect-jdbc-es:1.0";
    private static final String KAFKA_IMAGE = "confluentinc/cp-kafka:6.2.1";

    private static final String createTableSql = "CREATE TABLE IF NOT EXISTS test_table (id int primary key, first_name varchar(30), last_name varchar(50), days_worked double precision)";
    private static final String dropTableSql = "DROP TABLE test_table";

    private static KafkaContainer kafkaContainer;
    private static ElasticsearchContainer esContainer;
    private static DebeziumContainer kafkaConnectContainer;
    private static GenericContainer<?> cdcsdkContainer;

    private static ConnectorConfiguration sinkConfig;
    private static RestClient restClient;

    private static Network containerNetwork;

    private static final NodeSelector INGEST_NODE_SELECTOR = nodes -> {
        final Iterator<Node> iterator = nodes.iterator();
        while (iterator.hasNext()) {
            Node node = iterator.next();

            if (node.getRoles() != null && node.getRoles().isIngest() == false) {
                iterator.remove();
            }
        }
    };

    private static ConnectorConfiguration getEsSinkConfiguration(String connectionUrl) throws Exception {
        return ConnectorConfiguration.create()
                .with("connector.class", "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector")
                .with("tasks.max", 1)
                .with("topics", "dbserver1.public.test_table")
                .with("connection.url", connectionUrl)
                .with("transforms", "key")
                .with("transforms.key.type", "org.apache.kafka.connect.transforms.ExtractField$Key")
                .with("transforms.key.field", "id")
                .with("key.ignore", "false")
                .with("type.name", "test_table");
    }

    public static SSLContext createContextFromCaCert(byte[] certAsBytes) {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            Certificate trustedCa = factory.generateCertificate(new ByteArrayInputStream(certAsBytes));
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
            return sslContextBuilder.build();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        containerNetwork = Network.newNetwork();

        kafkaContainer = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withNetworkAliases("kafka").withNetwork(containerNetwork);

        kafkaConnectContainer = new DebeziumContainer(KAFKA_CONNECT_IMAGE).withKafka(kafkaContainer).dependsOn(kafkaContainer).withNetwork(containerNetwork);

        esContainer = new ElasticsearchContainer(ELASTIC_SEARCH_IMAGE).withNetwork(containerNetwork).withExposedPorts(9200)
                .withPassword("password");
        // esContainer.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*\"message\":\"started\".*"));
        esContainer.getEnvMap().remove("xpack.security.enabled");
        esContainer.withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
        kafkaContainer.start();
        kafkaConnectContainer.start();
        try {
            esContainer.start();
        }
        catch (Exception e) {
            System.out.println(esContainer.getLogs());
            throw e;
        }

        System.out.println();
        // Sleep for enough time so as to analyze the ES logs
        Thread.sleep(50000);

        TestHelper.setHost(InetAddress.getLocalHost().getHostAddress());
        TestHelper.setBootstrapServer(kafkaContainer.getNetworkAliases().get(0) + ":9092");

        sinkConfig = getEsSinkConfiguration(InetAddress.getLocalHost().getHostAddress() + ":" + esContainer.getMappedPort(9200));
        kafkaConnectContainer.registerConnector("es-sink-connector", sinkConfig);

        TestHelper.execute(createTableSql);

        cdcsdkContainer = TestHelper.getCdcsdkContainerForKafkaSink();
        cdcsdkContainer.withNetwork(containerNetwork);
        cdcsdkContainer.start();
    }

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {

    }

    @AfterAll
    public static void afterAll() throws Exception {
        esContainer.stop();
        kafkaConnectContainer.stop();
        cdcsdkContainer.stop();
        kafkaContainer.stop();
        TestHelper.execute(dropTableSql);
    }

    @Test
    public void testElasticsearchSink() throws Exception {
        System.out.println("Container stats: ");
        System.out.println("Kafka: " + kafkaContainer.isRunning());
        System.out.println("Kafka Connect: " + kafkaConnectContainer.isRunning());
        System.out.println("ElasticSearch: " + esContainer.isRunning());

        // byte[] certAsBytes = esContainer.copyFileFromContainer("/usr/share/elasticsearch/config/certs/http_ca.crt", InputStream::readAllBytes);
        HttpHost httpHost = new HttpHost(InetAddress.getLocalHost().getHostAddress(), esContainer.getMappedPort(9200), "http");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "password"));

        final RestClientBuilder restClientBuilder = RestClient.builder(httpHost);

        restClientBuilder.setHttpClientConfigCallback(clientBuilder -> {
            // clientBuilder.setSSLContext(createContextFromCaCert(certAsBytes));
            clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return clientBuilder;
        });

        restClientBuilder.setNodeSelector(INGEST_NODE_SELECTOR);

        restClient = restClientBuilder.build();

        Response response = restClient.performRequest(new Request("GET", "/_cluster/health"));
        System.out.println(response.toString());
    }
}

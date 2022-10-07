package hu.ptomi.complex.consumer;

import hu.ptomi.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Check OpenSearch ui here: http://localhost:5601/app/home#/
 * (1) check indices: http://localhost:5601/app/opensearch_index_management_dashboards#/indices?
 * (2) create index pattern for the index: http://localhost:5601/app/management/opensearch-dashboards/indexPatterns
 * (3) discover the incoming logs: http://localhost:5601/app/discover#
 * <p>
 * https://opensearch.org/docs/latest/#docker-quickstart
 * <p>
 * OpenSearch devtools:
 * PUT /my-first-index
 * {}
 * <p>
 * PUT /my-first-index/_doc/1
 * {"Description": "To be or not to be, that is the question."}
 * <p>
 * DELETE /my-first-index/_doc/1
 * DELETE /my-first-index
 */
public class OpenSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class);
    private static final ThreadFactory factory = Executors.defaultThreadFactory();

    public static void main(String[] args) {
        // setup kafka producer
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Configuration.KAFKA_CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        var consumer = new KafkaConsumer<String, String>(properties);

        // create os client and index if needed
        try (var osClient = createOpenSearchClient(); consumer) {
            createIndexIfNotExist(osClient);

            // add exit hook
            Runtime.getRuntime().addShutdownHook(kafkaCloser(Thread.currentThread(), consumer));

            consumer.subscribe(Collections.singleton(Configuration.KAFKA_TOPIC));

            while (true) {
                var records = consumer.poll(Duration.ofMillis(3 * 1_000));
                logger.info("Received: " + records.count() + " records from kafka");

                for (ConsumerRecord<String, String> r : records) {
                    try {
                        osClient.index(
                                new IndexRequest(Configuration.OS_INDEX).source(r.value(), XContentType.JSON),
                                RequestOptions.DEFAULT
                        );
                    } catch (Exception e) {
                        logger.warn("invalid document, skipping");
                    }
                }
            }

        } catch (IOException e) {
            logger.error("os client io error during close!", e);
        } catch (WakeupException e) {
            logger.info("shutdown initiated, stopping long poller.");
        } catch (Exception e) {
            logger.error("exception during poll!", e);
        }
    }

    private static <T, R> Thread kafkaCloser(Thread t, KafkaConsumer<T, R> consumer) {
        return factory.newThread(() -> {
            logger.info("initiate kafka consumer shutdown");
            consumer.wakeup();
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("error during join on caller!", e);
            }
        });
    }

    private static void createIndexIfNotExist(RestHighLevelClient client) throws IOException {
        try {
            client.indices().get(new GetIndexRequest(Configuration.OS_INDEX), RequestOptions.DEFAULT);
        } catch (OpenSearchStatusException e) {
            if (RestStatus.NOT_FOUND == e.status()) {
                logger.info("create os index: " + Configuration.OS_INDEX);
                client.indices().create(new CreateIndexRequest(Configuration.OS_INDEX), RequestOptions.DEFAULT);
            }
        }
    }

    private static RestHighLevelClient createOpenSearchClient() {
        var credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(Configuration.OS_ADMIN, Configuration.OS_ADMIN)
        );

        var builder = RestClient
                .builder(new HttpHost(Configuration.OS_HOST, Configuration.OS_PORT, "http"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }
}

package hu.ptomi.complex.producer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import hu.ptomi.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * SSE (server sent events): https://stream.wikimedia.org/v2/stream/recentchange
 * <p>
 * Check kafka ui at: http://localhost:8080/
 * <p>
 * Safe (Idempotent) Producer:
 * acks=all                                -> ensures data is properly replicated before an ack is received
 * min.in.sync.replicas                     -> ensures two brokers in ISR at least have the data after an ack
 * enable.idempotence=true                 -> duplicates are not introduced due to network retries
 * retries=MAX_INT                         -> retry until delivery.timeout.ms is reached
 * delivery.timeout.ms                     -> fail after retrying for 2 min
 * max.in.flight.requests.per.connection=5 -> ensure maximum perf. while keeping message orders
 */
public class WikimediaProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaProducer.class);
    private static final CountDownLatch shutdownBarrier = new CountDownLatch(1);
    private static final ThreadFactory factory = Executors.defaultThreadFactory();

    public static void main(String[] args) {
        // setup kafka producer
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // some perf. improvement: high throughput (at expense of a bit latency and CPU)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 kilobyte

        var kafkaProducer = new KafkaProducer<String, String>(properties);

        try {
            // add exit handler
            Runtime.getRuntime().addShutdownHook(kafkaCloser(kafkaProducer));

            // setup event listener
            var eventSource = new EventSource.Builder(
                    new WikimediaEventHandler<>(kafkaProducer),
                    new URI("https://stream.wikimedia.org/v2/stream/recentchange")
            ).build();

            // add exit handler
            Runtime.getRuntime().addShutdownHook(eventListenerCloser(eventSource));

            // start event listener
            eventSource.start();
            shutdownBarrier.await(); // jvm shutdown signal cause finally block to miss
        } catch (Throwable t) {
            log.error("Unexpected event listener error happened!", t);
        }
    }

    private static Thread eventListenerCloser(EventSource eventSource) {
        return factory.newThread(() -> {
            try {
                log.info("initiate event listener shutdown");
                eventSource.close();
                eventSource.awaitClosed(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.error("error during event listener shutdown!", e);
            } finally {
                shutdownBarrier.countDown();
            }
        });
    }

    private static <T, R> Thread kafkaCloser(KafkaProducer<T, R> producer) {
        return factory.newThread(() -> {
            log.info("initiate kafka producer shutdown");
            try {
                producer.flush();
                producer.close();
            } catch (Exception e) {
                log.error("error during kafka producer shutdown!", e);
            } finally {
                shutdownBarrier.countDown();
            }
        });
    }

    private record WikimediaEventHandler<T, R>(KafkaProducer<T, R> producer) implements EventHandler {

        private static final Logger log = LoggerFactory.getLogger(WikimediaEventHandler.class);

        @Override
        public void onOpen() {
            log.info("SSE stream opened");
        }

        @Override
        public void onClosed() {
            log.info("SSE stream closed");
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onMessage(String event, MessageEvent messageEvent) {
            log.info("SSE message received: " + messageEvent.getLastEventId());
            // async until buffer is full, then it will block the program to let the brokers catch up with this producer
            producer.send(new ProducerRecord<>(Configuration.KAFKA_TOPIC, (R) messageEvent.getData()));
        }

        @Override
        public void onComment(String comment) {
            log.info("SSE comment ignored: " + comment);
        }

        @Override
        public void onError(Throwable t) {
            log.error("Unexpected error happened!", t);
        }
    }

}

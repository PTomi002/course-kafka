package hu.ptomi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// auto.commit.interval.ms=5000 and enable.auto.commit=true will commit every 5 seconds during poll(...)
public class ConsumerWithAutoCommit {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithAutoCommit.class);

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Configuration.KAFKA_CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        // Do manual commit.
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        var consumer = new KafkaConsumer<String, String>(properties);

        final var mainThread = Thread.currentThread();
        Runtime
                .getRuntime()
                .addShutdownHook(new Thread(() -> {
                    log.info("Detected jvm shutdown, call wakeup() to interrupt the long polling thread.");
                    consumer.wakeup();
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        log.error("Error during join on caller!", e);
                    }
                }));

        consumer.subscribe(Collections.singleton(Configuration.KAFKA_TOPIC));

        try {
            while (true) {
                // make sure records processed before calling new poll
                // offset regularly committed at this line, before getting the new records
                var records = consumer.poll(Duration.ofMillis(1000));

                // more consumers can not read the same partition won't have duplicated messages
                // so consuming them here is a good practice, if exception is risen we won't commit the offset
                records.forEach(r -> log.info(
                        "Topic: " + r.topic() +
                                " Partition: " + r.partition() +
                                " Key: " + r.key() +
                                " Offset: " + r.offset() +
                                " Value: " + r.value()
                ));
                // manual commit: enable.auto.commit = false
//                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("Shutdown initiated, stopping long poller.");
        } catch (Exception e) {
            log.error("Unexpected exception during poll!", e);
        } finally {
            consumer.close();
        }
    }
}

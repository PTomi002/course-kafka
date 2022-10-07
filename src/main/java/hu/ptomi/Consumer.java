package hu.ptomi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Configuration.KAFKA_CONSUMER_GROUP);
        // With the auto.offset.reset configuration you can steer the behavior of your consumer (as part of a consumer group)
        //      in situations when your Consumer Group has never consumed and committed from a particular topic
        //      or the last committed offset from that Consumer Group was deleted (e.g. through cleanup policy).
        // none     ->  throws exception if no previous offset found
        // earliest ->  read from the oldest offset in the partition
        // latest   ->  read from the tail fo the topic
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
                log.info("Polling");
                // poll as many as we can and have
                var records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(r -> log.info(
                        "Topic: " + r.topic() +
                                " Partition: " + r.partition() +
                                " Key: " + r.key() +
                                " Offset: " + r.offset() +
                                " Value: " + r.value()
                ));
            }
        } catch (WakeupException e) {
            log.info("Shutdown initiated, stopping long poller.");
        } catch (Exception e) {
            log.error("Unexpected exception during poll!", e);
        } finally {
            // this commits offsets if needed
            consumer.close();
        }
    }
}

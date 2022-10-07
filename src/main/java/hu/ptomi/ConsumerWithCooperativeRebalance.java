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

// Change this:
//      partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
// To:
//      partition.assignment.strategy = [class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
// To prevent Stop-The-World-Event for the consumers and do it iteratively.
// 14:47:15.831 [main] INFO  o.a.k.c.c.i.ConsumerCoordinator - [Consumer clientId=consumer-my_first_consumer_group-1, groupId=my_first_consumer_group] Updating assignment with
//        Assigned partitions:                       [kafka_course_topic-0, kafka_course_topic-1, kafka_course_topic-2]
//        Current owned partitions:                  [kafka_course_topic-1]
//        Added partitions (assigned - owned):       [kafka_course_topic-0, kafka_course_topic-2]
//        Revoked partitions (owned - assigned):     []
public class ConsumerWithCooperativeRebalance {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithCooperativeRebalance.class);

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Configuration.KAFKA_CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Add assignment strategy
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

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
            consumer.close();
        }
    }
}

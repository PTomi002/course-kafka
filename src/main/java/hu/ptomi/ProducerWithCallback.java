package hu.ptomi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

import static java.util.Objects.isNull;

public class ProducerWithCallback {

    private static final String nl = System.lineSeparator();
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        var producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // key is NULL, round-robin distributed
            // BUT now we send data in BATCH, so it uses StickyPartitioner strategy, so all the 10 record goes to the same partition
            //      instead of round-robin, it has better performance
            // partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner
            var record = new ProducerRecord<String, String>(Configuration.KAFKA_TOPIC, UUID.randomUUID().toString());
            producer
                    .send(record, (meta, e) -> {
                        if (isNull(e)) {
                            log.info(
                                    nl + "Topic: " + meta.topic() +
                                            nl + "Offset: " + meta.offset() +
                                            nl + "Partition: " + meta.partition() +
                                            nl + "Timestamp: " + meta.timestamp()
                            );
                        } else
                            log.error("Could not produce record!", e);
                    });
//                        .get() <--- with this blocking operation all message would be round-robin distributed
        }

        producer.flush();
        producer.close();
    }
}

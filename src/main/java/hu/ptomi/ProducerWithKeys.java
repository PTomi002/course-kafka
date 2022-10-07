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

public class ProducerWithKeys {

    private static final String nl = System.lineSeparator();
    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class);

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        var producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // same key hashed into the same partition
            // check with:  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group_one
            //            GROUP           TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
            //            group_one       java_producer_one 0          5               5               0               -               -               -
            //            group_one       java_producer_one 1          0               0               0               -               -               -
            //            group_one       java_producer_one 2          5               5               0               -               -               -
            var record = new ProducerRecord<>(Configuration.KAFKA_TOPIC, (i % 2 == 0) ? "0" : "1", UUID.randomUUID().toString());
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
        }

        producer.flush();
        producer.close();
    }
}

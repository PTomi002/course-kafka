package hu.ptomi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.HOST);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        var producer = new KafkaProducer<String, String>(properties);

        // key is NULL, round-robin distributed
        var record = new ProducerRecord<String, String>(Configuration.TOPIC, UUID.randomUUID().toString());

        producer.send(record).get();

        producer.flush();
        producer.close();
    }
}

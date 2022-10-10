package hu.ptomi.wikimediaandopensearch.schemaregistry;

import hu.ptomi.Configuration;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static hu.ptomi.wikimediaandopensearch.schemaregistry.Person.newBuilder;

/**
 * Schema reg: http://localhost:8080/ui/clusters/docker/schemas
 */
public class PersonProducerAndConsumer {

    public static void main(String[] args) {
        // producer
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        // avro serializer setup
        properties.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, Configuration.KAFKA_SCHEMA_REGISTRY_HOST);
        var producer = new KafkaProducer<String, hu.ptomi.wikimediaandopensearch.schemaregistry.Person>(properties);

        IntStream.range(0, 1_000_000)
                .parallel()
                .forEach(i -> {
                    var person = newBuilder()
                            .setName("Tamas" + "_" + ThreadLocalRandom.current().nextInt())
                            .setAge(ThreadLocalRandom.current().nextInt(1, 99))
                            .setId(UUID.randomUUID().toString())
                            .build();
                    producer.send(
                            new ProducerRecord<>(
                                    Configuration.KAFKA_AVRO_TOPIC,
                                    null,
                                    person
                            )
                    );
                });

        producer.flush();
        producer.close();

        // consumer
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Configuration.KAFKA_AVRO_CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // avro deserializer setup
        properties.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, Configuration.KAFKA_SCHEMA_REGISTRY_HOST);
        // to cast to Person instead of GenericData
        properties.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        var consumer = new KafkaConsumer<String, hu.ptomi.wikimediaandopensearch.schemaregistry.Person>(properties);

        final var mainThread = Thread.currentThread();
        Runtime
                .getRuntime()
                .addShutdownHook(new Thread(() -> {
                    consumer.wakeup();
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }));

        consumer.subscribe(Collections.singleton(Configuration.KAFKA_AVRO_TOPIC));

        try {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(1_000));
                System.out.println("polled: " + records.count() + " record from kafka");
//                records.forEach(p -> System.out.println("Person fetched: " + p.value().toString()));
            }
        } catch (WakeupException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}

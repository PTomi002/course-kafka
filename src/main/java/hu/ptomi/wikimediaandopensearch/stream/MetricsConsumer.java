package hu.ptomi.wikimediaandopensearch.stream;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import hu.ptomi.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static org.apache.kafka.common.serialization.Serdes.String;

public class MetricsConsumer {

    private static Logger logger = LoggerFactory.getLogger(MetricsConsumer.class);
    private static final ThreadFactory factory = Executors.defaultThreadFactory();
    private static final CountDownLatch shutdownBarrier = new CountDownLatch(1);
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        var properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.KAFKA_HOST);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, Configuration.KAFKA_STREAMS_APP_ID);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass().getName());

        var builder = new StreamsBuilder();
        var stream = builder.<String, String>stream(Configuration.KAFKA_TOPIC);

        // add processors
        new BotCountProcessor(stream);

        var topology = builder.build();
        logger.info("Stream topology: " + topology.describe());

        try (var streams = new KafkaStreams(topology, properties)) {
            Runtime.getRuntime().addShutdownHook(factory.newThread(() -> {
                logger.info("jvm shutdown detected");
                streams.close();
                shutdownBarrier.countDown();
            }));

            streams.start();
            shutdownBarrier.await();
        } catch (Exception e) {
            logger.error("exception during streams!", e);
        }
    }

    private record BotCountProcessor(KStream<String, String> in) {
        private static final String STORE = "bot_count_store";

        private BotCountProcessor(KStream<String, String> in) {
            this.in = in;
            setup();
        }

        private void setup() {
            in
                    .mapValues(stream -> {
                                try {
                                    return JsonParser
                                            .parseString(stream)
                                            .getAsJsonObject()
                                            .get("bot")
                                            .getAsBoolean() ? "bot" : "not-bot";
                                } catch (Exception e) {
                                    return "not-bot";
                                }
                            }
                    )
                    .groupBy((k, botOrNot) -> botOrNot)
                    .count(Materialized.as(STORE))
                    .toStream()
                    .peek((key, value) -> logger.info("Grouped: " + key + "_" + value))
                    .mapValues((k, v) -> {
                        try {
                            return gson.toJson(Map.of(k, v));
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .to(Configuration.KAFKA_STREAMS_BOT_COUNT_TOPIC);
        }
    }

}

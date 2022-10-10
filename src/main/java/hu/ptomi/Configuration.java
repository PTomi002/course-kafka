package hu.ptomi;

public class Configuration {
    public static final String KAFKA_HOST = "127.0.0.1:9093";
    public static final String KAFKA_SCHEMA_REGISTRY_HOST = "http://127.0.0.1:8085";

    public static final String KAFKA_TOPIC = "kafka_course_t";
    public static final String KAFKA_CONSUMER_GROUP = "kafka_course_cg";
    public static final String KAFKA_AVRO_TOPIC = "kafka_person_t";
    public static final String KAFKA_AVRO_CONSUMER_GROUP = "kafka_person_cg";

    public static final String KAFKA_STREAMS_APP_ID = "kafka_course_stats_application";
    public static final String KAFKA_STREAMS_BOT_COUNT_TOPIC = KAFKA_TOPIC + "_" + "bot_count";

    public static final String OS_HOST = "localhost";
    public static final int OS_PORT = 9200;
    public static final String OS_ADMIN = "admin";
    public static final String OS_INDEX = "kafka_course_os_index";
}

# Apache Kafka Training

## Start Infra

`docker-compose -f docker-compose.kafka.yml up -d`<br />
`docker-compose -f docker-compose.kafka.yml rm`<br />
`docker-compose -f docker-compose.kafka.yml restart`<br />

`docker-compose -f docker-compose.opensearch.yml up -d`<br />
`docker-compose -f docker-compose.opensearch.yml rm`<br />
`docker-compose -f docker-compose.opensearch.yml restart`<br />

`docker exec -it kafka bash`

## CLI Commands

### Topic

`kafka-topics.sh --bootstrap-server localhost:9092 --list`<br />
`kafka-topics.sh --bootstrap-server localhost:9092 --create --topic first_topic --partitions 3`<br />
`kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic first_topic`<br />
`kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic first_topic`<br />

### Producer

`(*) auto-create topic with default settings and produce messages with NULL keys`<br />
`kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic`<br />
`>Hello World`<br />
`>My name is Tamas`

`(*) NON-NULL keys will be hashed into the same partition`<br />
`kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic --property parse.key=true --property key.separator=:`<br />
`>key_1:hello`<br />
`>key_2:world`<br />
`>key_1:hello_2`

### Consumer

`(*) reads from the tail of the partition, shows nothing`<br />
`(*) start a producer and write data to it, that is going to appear here`<br />
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic`<br />

`(*) messages will be in order WITHIN the partition but not BETWEEN the partitions`<br />
`(*) messages with NULL keys will be round-robin distributed between partitions, messages with KEYS will be hashed into the same partition`<br />
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning`<br />
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true`<br />

### Consumer Group

`(*) start three consumer in three parallel shell`<br />
`(*) messages will be spread between the same group: group_one, the second group will get them parallel`<br />
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group group_one`<br />
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group group_one`<br />
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group group_two`

`(*) consumer without a group will be a temporary consumer without offset commits`<br />
`kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list`<br />
`kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group_one`<br />

| Header        | Meaning           |
| ------------- |:-------------:|
| CURRENT-OFFSET      | actual message
| LOG-END-OFFSET      | max message
| LAG | diff between actual and max

### Reset Offset

`(*) use --execute instead of --dry-run`<br />
`kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --to-earliest --group group_one --dry-run --all-topics`<br />
`kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --to-earliest --group group_one --execute --all-topics`<br />
`kafka-consumer-groups.sh --bootstrap-server localhost:9092 --reset-offsets --shift-by -2 --group group_one --dry-run --all-topics`

| Header        | Meaning           |
| ------------- |:-------------:|
| to-earliest     | to the beginning
| shift-by -2      | backward with 2 commit
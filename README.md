### JsonToAvroConverter
A simple Kafka Streams application to convert topics containing homogenous JSON records to Avro Records.

#### Build
```bash
$ mvn clean package
```

#### Run
```bash
$ java -jar JsonToAvroConverter-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --properties-file /path/to/application.properties
    --avro-schema /path/to/avro-schema.avsc
    --source-topic clickstream-json
    --dest-topic clickstream-avro
```
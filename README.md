### JsonToAvroConverter
A simple Kafka Streams application to convert topics containing homogenous JSON records to Avro Records.

This application will read JSON records from a source topic and use the provided AVSC file to convert them to Avro records. It assumes the JSON fields are the same as the avsc fields. 
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

### Example

Example JSON:
```json
{
	"remote_user": "-",
	"request": "GET /images/track.png HTTP/1.1",
	"referrer": "-",
	"agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
	"bytes": "4006",
	"ip": "122.245.174.122",
	"time": "14/Apr/2018:16:11:57 -0400",
	"userid": 31,
	"_time": 1523736717787,
	"status": "200"
}
```

Corresponding Avro AVSC:
```json
{"namespace": "com.github.cjmatta.kafka.streams.avro",
"type": "record",
"name": "Clickstream",
"fields": [
    {"name": "remote_user", "type": "string"},
    {"name": "request", "type" : "string"},
    {"name": "referrer", "type": "string"},
    {"name": "agent", "type": "string"},
    {"name": "bytes", "type": "string"},
    {"name": "ip", "type": "string"},
    {"name": "time", "type": "string"},
    {"name": "userid", "type": "integer"},
    {"name": "_time", "type": "long"},
    {"name": "status", "type": "string"}
    ]
}
```
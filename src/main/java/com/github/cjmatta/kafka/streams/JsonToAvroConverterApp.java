package com.github.cjmatta.kafka.streams;

import io.confluent.kafka.serializers.*;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.kstream.KStream;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.codehaus.jackson.io.JsonStringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class JsonToAvroConverterApp {
  private static final Logger log = LoggerFactory.getLogger(JsonToAvroConverterApp.class);
  private static Properties props = null;
  private static KafkaStreams streams;
  private static Namespace namespace = null;
  private static String sourceTopic;
  private static String destTopic;

  private static JsonToGenericAvroRecordConverter jsonDeserializer;
  private static KafkaAvroSerializer jsonSerializer = new KafkaAvroSerializer();


  public static void main (String[] args) {
    ArgumentParser parser = argumentParser();

    namespace = parser.parseArgsOrFail(args);

    if(namespace.getAttrs().containsKey("properties_file")) {
      props = loadPropsOrFail(namespace.getString("properties_file"));
    } else {
      props = new Properties();
    }

//    Add any properties specified on the command line
    List<String> configProps = namespace.getList("configProperties");

    if (configProps != null) {
      for (String prop : configProps) {
        String[] parts = prop.split("=");
        if(parts.length != 2)
          throw new IllegalArgumentException("Invalid property: " + prop);
        props.put(parts[0], parts[1]);
      }
    }

    sourceTopic = namespace.getString("source_topic");
    destTopic = namespace.getString("dest_topic");

    runStreamsApp();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        stop();
      }
    }));
  }

  private static void runStreamsApp() {
    Schema avroSchema = readSchemaOrFail(namespace.getString("avro_schema"));
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(avroSchema);

    JsonSerde jsonSerde = new JsonSerde();
    jsonSerde.configure(
      Collections.singletonMap("json.value.type", JsonNode.class),
      false
    );

    GenericAvroSerde genericAvroValueSerde = new GenericAvroSerde();
    genericAvroValueSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, props.getProperty("schema.registry.url")),
      false
    );

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<String, JsonNode> jsonNodeKStream = builder.stream(sourceTopic, Consumed.with(Serdes.String(), jsonSerde));
    jsonNodeKStream.map(new KeyValueMapper<String, JsonNode, KeyValue<String, GenericRecord>>() {
      @Override
      public KeyValue<String, GenericRecord> apply(String key, JsonNode value) {
        try {
          return new KeyValue<>(key, converter.getGenericRecord(value));
        } catch (IOException e) {
          throw new RuntimeException(e.getMessage());
        }

      }
    }).to(destTopic, Produced.with(Serdes.String(), genericAvroValueSerde));

    streams = new KafkaStreams(builder.build(), props);
    streams.cleanUp();
    streams.start();

  }

  private static void stop()  {
    streams.close();

  }

  private static Schema readSchemaOrFail(String schemaPath) {
    String curDir = System.getProperty("user.dir");
    File schemaFile = new File(schemaPath);
    System.out.println("Current Directory: " + curDir);
    System.out.println("Current absolute directory: " + schemaFile.getAbsolutePath());

    try (FileInputStream schemaFileInputStream = new FileInputStream(schemaPath)) {
      return new Schema.Parser().parse(schemaFileInputStream);
    } catch (IOException e) {
      log.error("Error reading Avro Schema: ", e.getMessage());
      e.printStackTrace();
      e.getCause();
      System.exit(1);
    }
    return null;
  }

  private static Properties loadPropsOrFail(String filename) {
    Properties props = new Properties();
    try (InputStream propStream = new FileInputStream(filename)) {
      props.load(propStream);
    } catch (IOException e) {
      log.error("Error reading properties file: ", e.getMessage());
      System.exit(1);
    }
    return props;

  }

  private static ArgumentParser argumentParser() {
    ArgumentParser parser = ArgumentParsers.newFor("JsonToAvroConverterApp").build()
      .description("Kafka Streams app to convert JSON data on one topic to Avro data on the other.");

    parser.addArgument("--properties-file")
      .required(true)
      .type(String.class)
      .metavar("/path/to/application.properties")
      .help("Path to file containing this application's properties.");

    parser.addArgument("--config-property")
      .type(String.class)
      .metavar("PROP-NAME=PROP-VALUE")
      .nargs("+")
      .dest("configProperties")
      .help("List of properties in the form of PROP-NAME=PROP-VALUE");

    parser.addArgument("--avro-schema")
      .required(true)
      .type(String.class)
      .metavar("/path/to/avro.avsc")
      .help("The path to the avsc file containing the AVRO schema to use when serializing");

    parser.addArgument("--source-topic")
      .required(true)
      .type(String.class)
      .metavar("SOURCE-TOPIC")
      .help("The topic containing JSON");

    parser.addArgument("--dest-topic")
      .required(true)
      .type(String.class)
      .metavar("DEST-TOPIC")
      .help("The topic to output Avro");

    return parser;
  }

}

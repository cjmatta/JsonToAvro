package com.github.cjmatta.kafka.streams;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.*;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class JsonToAvroConverterApp {
  private static final Logger log = LoggerFactory.getLogger(JsonToAvroConverterApp.class);
  private static Properties props = null;
  private static Namespace namespace = null;
  private static String sourceTopic;
  private static String destTopic;

  private static KafkaJsonDeserializer jsonDeserializer = new KafkaJsonDeserializer();
  private static KafkaJsonSerializer jsonSerializer = new KafkaJsonSerializer();

  private static final Serde jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

  private static KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();




  public static void main (String[] args) {
    ArgumentParser parser = argumentParser();

    try {
      namespace = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    if(namespace.getAttrs().containsKey("properties_file")) {
      props = loadProps(namespace.getString("properties_file"));
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
  }

  private static void runStreamsApp() {

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, Object> inputStream = builder.stream(sourceTopic,
      Consumed.with(Serdes.String(), jsonSerde));
  }

  private static void stop()  {


  }

  private static Properties loadProps (String filename) {
    Properties props = new Properties();
    try (InputStream propStream = new FileInputStream(filename)) {
      props.load(propStream);
    } catch (IOException e) {
      log.error(e.getMessage());
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
      .nargs(1)
      .help("The path to the avsc file containing the AVRO schema to use when serializing");

    parser.addArgument("--source-topic")
      .required(true)
      .type(String.class)
      .metavar("SOURCE-TOPIC")
      .nargs(1)
      .help("The topic containing JSON");

    parser.addArgument("--dest-topic")
      .required(true)
      .type(String.class)
      .metavar("DEST-TOPIC")
      .nargs(1)
      .help("The topic to output Avro");

    return parser;
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.github.cjmatta.kafka.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class JsonToGenericAvroRecordConverterTest {
  private ObjectMapper objectMapper = new ObjectMapper();

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testStringDeserialize() throws IOException {
    String avsc = "{" +
      "\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\"," +
      "\"type\": \"record\"," +
      "\"name\": \"TestRecord\"," +
      "\"fields\": [{" +
        "\"name\": \"test\"," +
        "\"type\": \"string\"" +
      "}]" +
      "}";
    String json = "{\"test\": \"test\"}";

    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    assertEquals("test", record.get("test"));
  }

  @Test
  public void testIntDeserialize () throws IOException {
    String avsc = "{\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\",\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [{\"name\": \"test\", \"type\": \"int\"}]}";
    String json = "{\"test\": 123}";

    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    assertEquals(123, record.get("test"));
  }

  @Test
  public void testBooleanDeserialize () throws IOException {
    String avsc = "{\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\",\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [{\"name\": \"test\", \"type\": \"boolean\"}]}";
    String json = "{\"test\": true}";

    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    assertEquals(true, record.get("test"));
  }

  @Test
  public void testNullDeserialize () throws IOException {
    String avsc = "{\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\",\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [{\"name\": \"test\", \"type\": \"string\"}]}";
    String json = "{\"test\": null}";
    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    assertEquals("null", record.get("test"));
  }

  @Test
  public void testDefaultValueDeserialize () throws IOException {
    String avsc = "{\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\",\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [{\"name\": \"test\", \"type\": \"string\", \"default\": \"john doe\"}]}";
    String json = "{\"test\": null}";
    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    assertEquals("john doe", record.get("test"));
  }


  @Test
  public void testMultiRecordDeserialize () throws IOException {
    String avsc = "{" +
      "\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\"," +
      "\"type\": \"record\"," +
      "\"name\": \"TestRecord\"," +
      "\"fields\": [{" +
      "\"name\": \"test\"," +
      "\"type\": \"string\"" +
      "}, {" +
      "\"name\": \"othertest\"," +
      "\"type\": \"int\"" +
      "}]" +
      "}";
    String json = "{\"test\": \"test\", \"othertest\": 123}";
    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    assertEquals("test", record.get("test"));
    assertEquals(123, record.get("othertest"));
  }

  @Test
  public void testArrayRecordDeserialize () throws IOException {
    String avsc = "{" +
      "\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\"," +
      "\"type\": \"record\"," +
      "\"name\": \"TestRecord\"," +
      "\"fields\": [{" +
      "\"name\": \"test\"," +
      "\"type\": {" +
      "\"type\": \"array\"," +
      "\"items\": \"string\"" +
      "}" +
      "}]" +
      "}";
    String json = "{\"test\": [\"one\", \"two\", \"three\"]}";
    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    List<String> arrayContents = (List<String>) record.get("test");
    assertEquals(3, arrayContents.size());
    assertEquals("one", arrayContents.get(0));
    assertEquals("two", arrayContents.get(1));
    assertEquals("three", arrayContents.get(2));
  }

  @Test
  public void testMapRecordDeserialize () throws IOException {
    String avsc = "{" +
      "\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\"," +
      "\"type\": \"record\"," +
      "\"name\": \"TestRecord\"," +
      "\"fields\": [{" +
      "\"name\": \"test\"," +
      "\"type\": {" +
      "\"type\": \"map\"," +
      "\"values\": \"string\"" +
      "}" +
      "}]" +
      "}";
    String json = "{" +
      "\"test\": {" +
      "\"item1key\": \"item1value\"," +
      "\"item2key\": \"item2value\"" +
      "}" +
      "}";
    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    Map<String, Object> mapContents = (Map<String, Object>) record.get("test");
    assertEquals("item1value", mapContents.get("item1key"));

  }

  @Test
  public void testListOfGenericRecordsDeserialize () throws IOException {
    String avsc = "{" +
      "\"name\": \"Parent\"," +
      "\"type\": \"record\"," +
      "\"fields\": [{" +
      "\"name\": \"children\"," +
      "\"type\": {" +
      "\"type\": \"array\"," +
      "\"items\": {" +
      "\"name\": \"Child\"," +
      "\"type\": \"record\"," +
      "\"fields\": [{" +
      "\"name\": \"name\"," +
      "\"type\": \"string\"" +
      "}]" +
      "}" +
      "}" +
      "}]" +
      "}";
    String json = "{" +
      "\"children\": [{" +
      "\"name\": \"Sam\"" +
      "}, {" +
      "\"name\": \"Alice\"" +
      "}]" +
      "}";
    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    List<GenericRecord> childrenRecord = (List<GenericRecord>) record.get("children");
    assertTrue(childrenRecord.get(0) != null);
    assertEquals("Sam", childrenRecord.get(0).get("name"));
  }

  @Test
  public void testEnumDeserialize () throws IOException {
    String avsc = "{" +
      "\"name\": \"PlayingCard\"," +
      "\"type\": \"record\"," +
      "\"fields\": [{" +
      "\"name\": \"suit\"," +
      "\"type\": {" +
      "\"type\": \"enum\"," +
      "\"name\": \"suit\"," +
      "\"symbols\": [\"CLUB\", \"HEART\", \"SPADE\", \"DIAMOND\"]" +
      "}" +
      "}]" +
      "}";
    String json = "{\"suit\": \"CLUB\"}";
    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    GenericRecord record = converter.getGenericRecord(jsonNode);
    assertEquals("CLUB", record.get("suit"));
  }

  @Test
  public void testEnumBadEntryDeserialize () throws IOException {
    String avsc = "{" +
      "\"name\": \"PlayingCard\"," +
      "\"type\": \"record\"," +
      "\"fields\": [{" +
      "\"name\": \"suit\"," +
      "\"type\": {" +
      "\"type\": \"enum\"," +
      "\"name\": \"suit\"," +
      "\"symbols\": [\"CLUB\", \"HEART\", \"SPADE\", \"DIAMOND\"]" +
      "}" +
      "}]" +
      "}";
    String json = "{\"suit\": \"CLOVER\"}";
    JsonNode jsonNode = objectMapper.readTree(json);
    JsonToGenericAvroRecordConverter converter = new JsonToGenericAvroRecordConverter(new Schema.Parser().parse(avsc));
    exception.expect(AvroTypeException.class);
    GenericRecord record = converter.getGenericRecord(jsonNode);
  }




}
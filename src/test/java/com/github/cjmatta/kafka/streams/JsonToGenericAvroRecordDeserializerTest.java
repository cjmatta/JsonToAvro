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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class JsonToGenericAvroRecordDeserializerTest {

  @Test
  public void testStringDeserialize () {
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
    JsonToGenericAvroRecordDeserializer deserializer = new JsonToGenericAvroRecordDeserializer(new Schema.Parser().parse(avsc));
    GenericRecord record = deserializer.deserialize(null, json.getBytes());
    assertEquals("test", record.get("test"));
  }

  @Test
  public void testIntDeserialize () {
    String avsc = "{\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\",\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [{\"name\": \"test\", \"type\": \"int\"}]}";
    String json = "{\"test\": 123}";
    JsonToGenericAvroRecordDeserializer deserializer = new JsonToGenericAvroRecordDeserializer(new Schema.Parser().parse(avsc));
    GenericRecord record = deserializer.deserialize(null, json.getBytes());
    assertEquals(123, record.get("test"));
  }

  @Test
  public void testBooleanDeserialize () {
    String avsc = "{\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\",\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [{\"name\": \"test\", \"type\": \"boolean\"}]}";
    String json = "{\"test\": true}";
    JsonToGenericAvroRecordDeserializer deserializer = new JsonToGenericAvroRecordDeserializer(new Schema.Parser().parse(avsc));
    GenericRecord record = deserializer.deserialize(null, json.getBytes());
    assertEquals(true, record.get("test"));
  }

  @Test
  public void testNullDeserialize () {
    String avsc = "{\"namespace\": \"com.github.cjmatta.kafka.streams.avro.test\",\"type\": \"record\",\"name\": \"TestRecord\",\"fields\": [{\"name\": \"test\", \"type\": \"string\"}]}";
    String json = "{\"test\": null}";
    JsonToGenericAvroRecordDeserializer deserializer = new JsonToGenericAvroRecordDeserializer(new Schema.Parser().parse(avsc));
    GenericRecord record = deserializer.deserialize(null, json.getBytes());
    assertEquals("null", record.get("test"));
  }

  @Test
  public void testMultiRecordDeserialize () {
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
    JsonToGenericAvroRecordDeserializer deserializer = new JsonToGenericAvroRecordDeserializer(new Schema.Parser().parse(avsc));
    GenericRecord record = deserializer.deserialize(null, json.getBytes());
    assertEquals("test", record.get("test"));
    assertEquals(123, record.get("othertest"));
  }

  @Test
  public void testArrayRecordDeserialize () {
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
    JsonToGenericAvroRecordDeserializer deserializer = new JsonToGenericAvroRecordDeserializer(new Schema.Parser().parse(avsc));
    GenericRecord record = deserializer.deserialize(null, json.getBytes());
    List<String> arrayContents = (List<String>) record.get("test");
    assertEquals(3, arrayContents.size());
    assertEquals("one", arrayContents.get(0));
    assertEquals("two", arrayContents.get(1));
    assertEquals("three", arrayContents.get(2));
  }



}
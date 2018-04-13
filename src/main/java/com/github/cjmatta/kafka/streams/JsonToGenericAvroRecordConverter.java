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
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonToGenericAvroRecordConverter {
  private Logger log = LoggerFactory.getLogger(JsonToGenericAvroRecordConverter.class);
  private Schema schema;

  public JsonToGenericAvroRecordConverter(Schema schema) { this.schema = schema; }


  public GenericRecord getGenericRecord (JsonNode jsonNode) throws IOException {

    GenericRecord genericRecord = new GenericData.Record(this.schema);

    for (Schema.Field field: schema.getFields()) {
      JsonNode fieldJsonNode = jsonNode.get(field.name());
      if (fieldJsonNode == null) {
        genericRecord.put(field.name(), null);
      } else {
        genericRecord.put(field.name(), enforceType(field.schema(), fieldJsonNode));
      }
    }

    return genericRecord;
  }

  private Object enforceType (Schema schema, JsonNode jsonNode) throws IOException {
    switch (schema.getType()) {
      case BOOLEAN:
        return jsonNode.asBoolean();
      case INT:
        return jsonNode.asInt();
      case FLOAT:
        return jsonNode.asDouble();
      case STRING:
        if(jsonNode.isTextual()) {
          return jsonNode.asText();
        } else {
          return jsonNode.toString();
        }
      case ENUM:
        String value = jsonNode.toString();
        List<String> enumSymbols = schema.getEnumSymbols();
        if (!enumSymbols.contains(value)) {
          throw new IOException("Value" + value + "not in Avro Enum symbols!");
        }
      case FIXED:
        return jsonNode.toString();
      case LONG:
        return jsonNode.asLong();
      case ARRAY:
        Schema elementSchema = schema.getElementType();
        List<Object> elements = new ArrayList<>();

        for(int i = 0; i < jsonNode.size(); i++) {
          elements.add(enforceType(elementSchema, jsonNode.get(i)));
        }

        return elements;
      case RECORD:
        GenericRecord genericRecord = new GenericData.Record(schema);
        for (Schema.Field field: schema.getFields()) {
          JsonNode fieldJsonNode = jsonNode.get(field.name());
          if (fieldJsonNode == null) {
            genericRecord.put(field.name(), null);
          } else {
            genericRecord.put(field.name(), enforceType(field.schema(), fieldJsonNode));
          }
        }
      case NULL:
        return null;
      default:
        throw new IOException("Type is not supported.");
    }
  }

}

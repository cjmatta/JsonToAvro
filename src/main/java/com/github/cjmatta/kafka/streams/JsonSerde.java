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
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerde implements Serde<JsonNode> {
  private final Serde<JsonNode> inner;

  public JsonSerde() {
    this.inner = Serdes.serdeFrom(new KafkaJsonSerializer<JsonNode>(), new KafkaJsonDeserializer<JsonNode>());

  }

  @Override
  public void configure(Map serdeConfig, boolean isKey) {
    inner.serializer().configure(serdeConfig, isKey);
    inner.deserializer().configure(serdeConfig, isKey);
   }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

  @Override
  public Serializer<JsonNode> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<JsonNode> deserializer() {
    return inner.deserializer();
  }
}

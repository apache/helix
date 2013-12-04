package org.apache.helix.controller.serializer;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;

import org.apache.helix.HelixException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Default serializer implementation for converting to/from strings. Uses the Jackson JSON library
 * to do the conversion
 */
public class DefaultStringSerializer implements StringSerializer {

  private static Logger logger = Logger.getLogger(DefaultStringSerializer.class);

  @Override
  public <T> String serialize(final T data) {
    if (data == null) {
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    StringWriter sw = new StringWriter();
    try {
      mapper.writeValue(sw, data);
    } catch (Exception e) {
      logger.error("Exception during payload data serialization.", e);
      throw new HelixException(e);
    }
    return sw.toString();
  }

  @Override
  public <T> T deserialize(final Class<T> clazz, final String string) {
    if (string == null || string.length() == 0) {
      return null;
    }

    ObjectMapper mapper = new ObjectMapper();
    ByteArrayInputStream bais = new ByteArrayInputStream(string.getBytes());

    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_CREATORS, true);
    deserializationConfig.set(DeserializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    try {
      T payload = mapper.readValue(bais, clazz);
      return payload;
    } catch (Exception e) {
      logger.error("Exception during deserialization of payload bytes: " + string, e);
      return null;
    }
  }
}

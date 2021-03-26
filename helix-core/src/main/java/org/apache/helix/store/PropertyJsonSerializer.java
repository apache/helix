package org.apache.helix.store;

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

import java.io.ByteArrayInputStream;
import java.io.StringWriter;

import org.apache.helix.HelixException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyJsonSerializer<T> implements PropertySerializer<T> {
  static private Logger LOG = LoggerFactory.getLogger(PropertyJsonSerializer.class);
  private final Class<T> _clazz;

  public PropertyJsonSerializer(Class<T> clazz) {
    _clazz = clazz;
  }

  @Override
  public byte[] serialize(T data) throws PropertyStoreException {
    ObjectMapper mapper = new ObjectMapper();

    SerializationConfig serializationConfig = mapper.getSerializationConfig()
            .with(SerializationFeature.INDENT_OUTPUT)
            .with(MapperFeature.AUTO_DETECT_FIELDS, true)
            .with(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    StringWriter sw = new StringWriter();

    try {
      mapper.writeValue(sw, data);

      if (sw.toString().getBytes().length > ZNRecord.SIZE_LIMIT) {
        throw new HelixException("Data size larger than 1M. Write empty string to zk.");
      }
      return sw.toString().getBytes();

    } catch (Exception e) {
      LOG.error("Error during serialization of data (first 1k): "
          + sw.toString().substring(0, 1024), e);
    }

    return new byte[] {};
  }

  @Override
  public T deserialize(byte[] bytes) throws PropertyStoreException {
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig()
            .with(MapperFeature.AUTO_DETECT_FIELDS, true)
            .with(MapperFeature.AUTO_DETECT_SETTERS, true)
            .with(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    try {
      T value = mapper.readValue(bais, _clazz);
      return value;
    } catch (Exception e) {
      LOG.error("Error during deserialization of bytes: " + new String(bytes), e);
    }

    return null;
  }

}

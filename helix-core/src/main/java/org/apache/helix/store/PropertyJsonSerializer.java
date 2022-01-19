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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.helix.HelixException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyJsonSerializer<T> implements PropertySerializer<T> {
  static private Logger LOG = LoggerFactory.getLogger(PropertyJsonSerializer.class);
  private static ObjectMapper mapper = new ObjectMapper();
  private final Class<T> _clazz;

  public PropertyJsonSerializer(Class<T> clazz) {
    _clazz = clazz;
  }

  @Override
  public byte[] serialize(T data) throws PropertyStoreException {
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.enable(MapperFeature.AUTO_DETECT_FIELDS);
    mapper.enable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
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
    mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, true);
    mapper.configure(MapperFeature.AUTO_DETECT_SETTERS, true);
    mapper.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    try {
      T value = mapper.readValue(bais, _clazz);
      return value;
    } catch (Exception e) {
      LOG.error("Error during deserialization of bytes: " + new String(bytes), e);
    }

    return null;
  }

}

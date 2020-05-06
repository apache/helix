package org.apache.helix.manager.zk;

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
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.util.GZipCompressionUtil;
import org.apache.helix.util.ZNRecordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

public class ZNRecordSerializer implements ZkSerializer {
  private static Logger LOG = LoggerFactory.getLogger(ZNRecordSerializer.class);

  private static int getListFieldBound(ZNRecord record) {
    int max = Integer.MAX_VALUE;
    if (record.getSimpleFields().containsKey(ZNRecord.LIST_FIELD_BOUND)) {
      String maxStr = record.getSimpleField(ZNRecord.LIST_FIELD_BOUND);
      try {
        max = Integer.parseInt(maxStr);
      } catch (Exception e) {
        LOG.error("IllegalNumberFormat for list field bound: " + maxStr);
      }
    }
    return max;
  }

  @Override
  public byte[] serialize(Object data) {
    if (!(data instanceof ZNRecord)) {
      // null is NOT an instance of any class
      LOG.error("Input object must be of type ZNRecord but it is " + data
          + ". Will not write to zk");
      throw new HelixException("Input object is not of type ZNRecord (was " + data + ")");
    }

    ZNRecord record = (ZNRecord) data;

    // apply retention policy
    int max = getListFieldBound(record);
    if (max < Integer.MAX_VALUE) {
      Map<String, List<String>> listMap = record.getListFields();
      for (String key : listMap.keySet()) {
        List<String> list = listMap.get(key);
        if (list.size() > max) {
          listMap.put(key, list.subList(0, max));
        }
      }
    }

    // do serialization
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] serializedBytes;
    boolean isCompressed = false;

    try {
      mapper.writeValue(baos, data);
      serializedBytes = baos.toByteArray();
      // apply compression if needed
      if (ZNRecordUtil.shouldCompress(record, serializedBytes.length)) {
        serializedBytes = GZipCompressionUtil.compress(serializedBytes);
        isCompressed = true;
      }
    } catch (Exception e) {
      LOG.error(
          "Exception during data serialization. ZNRecord ID: {} will not be written to zk.",
          record.getId(), e);
      throw new HelixException(e);
    }

    int writeSizeLimit = ZNRecordUtil.getSerializerWriteSizeLimit();
    if (serializedBytes.length > writeSizeLimit) {
      LOG.error("Data size: {} is greater than {} bytes, is compressed: {}, ZNRecord.id: {}."
              + " Data will not be written to Zookeeper.", serializedBytes.length, writeSizeLimit,
          isCompressed, record.getId());
      throw new HelixException(
          "Data size: " + serializedBytes.length + " is greater than " + writeSizeLimit
              + " bytes, is compressed: " + isCompressed + ", ZNRecord.id: " + record.getId());
    }

    return serializedBytes;
  }

  @Override
  public Object deserialize(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      // reading a parent/null node
      return null;
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

    ObjectMapper mapper = new ObjectMapper();
    DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, true);
    deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    try {
      //decompress the data if its already compressed
      if (GZipCompressionUtil.isCompressed(bytes)) {
        byte[] uncompressedBytes = GZipCompressionUtil.uncompress(bais);
        bais = new ByteArrayInputStream(uncompressedBytes);
      }

      return mapper.readValue(bais, ZNRecord.class);
    } catch (Exception e) {
      LOG.error("Exception during deserialization of bytes: {}", new String(bytes), e);
      return null;
    }
  }
}

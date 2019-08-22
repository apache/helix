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

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZNRecordJacksonSerializer serializes ZNRecord objects into a byte array using MessagePack's
 * serializer. Note that this serializer doesn't check for the size of the resulting binary.
 */
public class ZNRecordJacksonSerializer implements ZkSerializer {
  private static Logger LOG = LoggerFactory.getLogger(ZNRecordJacksonSerializer.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public byte[] serialize(Object record) throws ZkMarshallingError {
    if (!(record instanceof ZNRecord)) {
      // null is NOT an instance of any class
      LOG.error(
          "Input object must be of type ZNRecord but it is " + record + ". Will not write to zk");
      throw new HelixException("Input object is not of type ZNRecord (was " + record + ")");
    }
    ZNRecord znRecord = (ZNRecord) record;

    try {
      return OBJECT_MAPPER.writeValueAsBytes(znRecord);
    } catch (Exception e) {
      throw new HelixException(
          String.format("Exception during serialization. ZNRecord id: %s", znRecord.getId()), e);
    }
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError {
    if (bytes == null || bytes.length == 0) {
      // reading a parent/null node
      return null;
    }

    ZNRecord record;
    try {
      record = OBJECT_MAPPER.readValue(bytes, ZNRecord.class);
    } catch (Exception e) {
      throw new HelixException("Exception during deserialization!", e);
    }
    return record;
  }
}

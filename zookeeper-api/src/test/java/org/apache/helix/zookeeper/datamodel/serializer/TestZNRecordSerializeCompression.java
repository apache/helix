package org.apache.helix.zookeeper.datamodel.serializer;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZNRecordSerializeCompression {
  /*
   * Tests data serializing when auto compression is enabled.
   * Two cases:
   * 1. compression threshold is not set
   * --> default size (1 MB) is used.
   * 2. compression threshold is set
   * --> serialized data is compressed.
   */
  @Test
  public void testZNRecordSerializerCompressThreshold() {
    // Backup properties for later resetting.
    final String compressionThresholdProperty =
        System.getProperty(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES);

    // Unset compression threshold property so default threshold is used.
    System.clearProperty(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES);

    Assert.assertNull(
        System.getProperty(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES));

    verifyAutoCompression(false, false, false);

    // 2. Set threshold so serialized data is greater than the threshold but compressed data
    // is smaller than the threshold.
    int compressionThreshold = 2000;
    System.setProperty(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES,
        String.valueOf(compressionThreshold));

    // Verify auto compression is done.
    verifyAutoCompression(true, true, false);

    // 3. Set threshold both serialized data and compressed data are greater than the threshold.
    compressionThreshold = 10;
    System.setProperty(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES,
        String.valueOf(compressionThreshold));

    // Verify ZkClientException is thrown because compressed data is larger than threshold.
    verifyAutoCompression(true, true, true);

    // Reset: add the properties back to system properties if they were originally available.
    if (compressionThresholdProperty != null) {
      System.setProperty(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES,
          compressionThresholdProperty);
    } else {
      System.clearProperty(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES);
    }
  }

  private void verifyAutoCompression(boolean greaterThanThreshold, boolean compressionExpected,
      boolean exceptionExpected) {
    int compressionThreshold = Integer
        .getInteger(ZkSystemPropertyKeys.ZNRECORD_SERIALIZER_COMPRESS_THRESHOLD_BYTES,
            ZNRecord.SIZE_LIMIT);

    ZNRecord record = createZNRecord(10);

    // Makes sure the length of serialized bytes is greater than compression threshold to
    // satisfy the condition: serialized bytes' length exceeds the threshold.
    byte[] preCompressedBytes = serialize(record);
    Assert.assertEquals(preCompressedBytes.length > compressionThreshold, greaterThanThreshold);

    ZkSerializer zkSerializer = new ZNRecordSerializer();

    byte[] bytes;
    try {
      bytes = zkSerializer.serialize(record);
      Assert.assertFalse(exceptionExpected);
    } catch (ZkClientException ex) {
      Assert.assertTrue(exceptionExpected, "Should not throw ZkClientException.");
      Assert.assertTrue(
          ex.getMessage().contains(" is greater than " + compressionThreshold + " bytes"));
      // No need to verify following asserts as bytes data is not returned.
      return;
    }

    // Per auto compression being enabled(or not), verify whether serialized data
    // is compressed or not.
    Assert.assertEquals(GZipCompressionUtil.isCompressed(bytes), compressionExpected);
    Assert.assertEquals(preCompressedBytes.length != bytes.length, compressionExpected);

    // Verify serialized bytes could be correctly deserialized.
    Assert.assertEquals(zkSerializer.deserialize(bytes), record);
  }

  private ZNRecord createZNRecord(final int recordSize) {
    ZNRecord record = new ZNRecord("record");
    for (int i = 0; i < recordSize; i++) {
      String field = "field-" + i;
      record.setSimpleField(field, field);
      record.setListField(field, new ArrayList<>(recordSize));
      for (int j = 0; j < recordSize; j++) {
        record.getListField(field).add("field-" + j);
      }

      record.setMapField(field, new TreeMap<>());
      for (int j = 0; j < recordSize; j++) {
        String mapField = "field-" + j;
        record.getMapField(field).put(mapField, mapField);
      }
    }

    return record;
  }

  // Simulates serializing so we can check the size of serialized bytes.
  // Returns raw serialized bytes before being compressed.
  private byte[] serialize(Object data) {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, true);
    serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] serializedBytes = new byte[0];

    try {
      mapper.writeValue(baos, data);
      serializedBytes = baos.toByteArray();
    } catch (IOException e) {
      Assert.fail("Can not serialize data.", e);
    }

    return serializedBytes;
  }
}

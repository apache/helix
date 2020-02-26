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
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZNRecordSerializer {
  /*
   * Tests data serializing when auto compression is disabled. If the system property for
   * auto compression is set to false or not set, auto compression is disabled.
   */
  @Test
  public void testAutoCompressionDisabled() {
    // Backup properties for later resetting.
    final String compressionEnabledProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED);
    final String compressionThresholdProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES);

    // Prepare system properties to disable auto compression.
    final int compressionThreshold = 100;
    System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES,
        String.valueOf(compressionThreshold));

    // 1. Unset the auto compression enabled property so auto compression is disabled.
    System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED);

    // Verify auto compression is disabled.
    Assert.assertFalse(
        Boolean.getBoolean(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED));
    verifyAutoCompression(false, true, false);

    // 2. Set the auto compression enabled property to false so auto compression is disabled.
    System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED, "false");

    // Verify auto compression is disabled.
    Assert.assertFalse(
        Boolean.getBoolean(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED));
    verifyAutoCompression(false, true, false);

    // Reset: add the properties back to system properties if they were originally available.
    if (compressionEnabledProperty != null) {
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED,
          compressionEnabledProperty);
    }
    if (compressionThresholdProperty != null) {
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES,
          compressionThresholdProperty);
    }
  }

  /*
   * Tests data serializing when auto compression is disabled.
   * Two cases:
   * 1. auto compression threshold is not set
   * --> default size (1 MB) is used.
   * 2. auto compression threshold is set
   * --> serialized data is compressed.
   */
  @Test(dependsOnMethods = "testAutoCompressionDisabled")
  public void testAutoCompressionEnabled() {
    // Backup properties for later resetting.
    final String compressionEnabledProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED);
    final String compressionThresholdProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES);

    // Set the auto compression enabled property to true so auto compression is enabled.
    System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED, "true");

    // 1. Unset compression threshold property so default threshold is used.
    System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES);

    Assert.assertNull(
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES));

    verifyAutoCompression(true, false, false);

    // 2. Set threshold to 100 so serialized data is greater than the threshold.
    final int compressionThreshold = 100;
    System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES,
        String.valueOf(compressionThreshold));

    // Verify auto compression is enabled.
    Assert.assertTrue(
        Boolean.getBoolean(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED));

    verifyAutoCompression(true, true, true);

    // Reset: add the properties back to system properties if they were originally available.
    if (compressionEnabledProperty != null) {
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED,
          compressionEnabledProperty);
    }
    if (compressionThresholdProperty != null) {
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES,
          compressionThresholdProperty);
    }
  }

  private void verifyAutoCompression(boolean compressionEnabled, boolean greaterThanThreshold,
      boolean compressionExpected) {
    int compressionThreshold = Integer
        .getInteger(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_THRESHOLD_BYTES,
            ZNRecord.SIZE_LIMIT);

    ZNRecord record = createZNRecord(10);

    // Makes sure the length of serialized bytes is greater than compression threshold to
    // satisfy the condition: serialized bytes' length exceeds the threshold.
    byte[] preCompressedBytes = serialize(record);
    Assert.assertEquals(preCompressedBytes.length > compressionThreshold, greaterThanThreshold);

    ZkSerializer zkSerializer = new ZNRecordSerializer();
    byte[] bytes = zkSerializer.serialize(record);

    // Per auto compression being enabled(or not), verify whether serialized data
    // is compressed or not.
    Assert.assertEquals(
        Boolean.getBoolean(ZkSystemPropertyKeys.ZK_SERIALIZER_AUTO_COMPRESSION_ENABLED),
        compressionEnabled);
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

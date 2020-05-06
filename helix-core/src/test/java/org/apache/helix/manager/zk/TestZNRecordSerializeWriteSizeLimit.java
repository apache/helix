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

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixException;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.ZNRecord;
import org.apache.helix.util.GZipCompressionUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZNRecordSerializeWriteSizeLimit {
  /*
   * Tests data serializing when auto compression is disabled. If the system property for
   * auto compression is set to "false", auto compression is disabled.
   */
  @Test
  public void testAutoCompressionDisabled() {
    // Backup properties for later resetting.
    final String compressionEnabledProperty =
        System.getProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED);
    final String compressionThresholdProperty =
        System.getProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);

    // Prepare system properties to disable auto compression.
    final int writeSizeLimit = 200 * 1024;
    System.setProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
        String.valueOf(writeSizeLimit));

    // 2. Set the auto compression enabled property to false so auto compression is disabled.
    System.setProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED, "false");

    // Verify auto compression is disabled.
    Assert.assertFalse(
        Boolean.getBoolean(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED));
    // Data size 300 KB > size limit 200 KB: exception expected.
    verifyAutoCompression(300, writeSizeLimit, true, false, true);

    // Data size 100 KB < size limit 200 KB: pass
    verifyAutoCompression(100, writeSizeLimit, false, false, false);

    // Reset: add the properties back to system properties if they were originally available.
    if (compressionEnabledProperty != null) {
      System.setProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED,
          compressionEnabledProperty);
    } else {
      System.clearProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED);
    }
    if (compressionThresholdProperty != null) {
      System.setProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          compressionThresholdProperty);
    } else {
      System.clearProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
    }
  }

  /*
   * Tests data serializing when write size limit is set.
   * Two cases:
   * 1. limit is not set
   * --> default size is used.
   * 2. limit is set
   * --> serialized data is checked by the limit: pass or throw ZkClientException.
   */
  @Test
  public void testZNRecordSerializerWriteSizeLimit() {
    // Backup properties for later resetting.
    final String writeSizeLimitProperty =
        System.getProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);

    // Unset write size limit property so default limit is used.
    System.clearProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);

    Assert.assertNull(
        System.getProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES));

    verifyAutoCompression(500, ZNRecord.SIZE_LIMIT, false, false, false);

    // 2. Set size limit so serialized data is greater than the size limit but compressed data
    // is smaller than the size limit.
    // Set it to 2000 bytes
    int writeSizeLimit = 2000;
    System.setProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
        String.valueOf(writeSizeLimit));

    // Verify auto compression is done.
    verifyAutoCompression(200, writeSizeLimit, true, true, false);

    // 3. Set size limit to be be less than default value. The default value will be used for write
    // size limit.
    writeSizeLimit = 2000;
    System.setProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
        String.valueOf(writeSizeLimit));

    // Verify ZkClientException is thrown because compressed data is larger than size limit.
    verifyAutoCompression(1000, writeSizeLimit, true, true, true);

    // Reset: add the properties back to system properties if they were originally available.
    if (writeSizeLimitProperty != null) {
      System.setProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          writeSizeLimitProperty);
    } else {
      System.clearProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
    }
  }

  private void verifyAutoCompression(int recordSize, int limit, boolean greaterThanThreshold,
      boolean compressionExpected, boolean exceptionExpected) {
    ZNRecord record = createZNRecord(recordSize);

    // Makes sure the length of serialized bytes is greater than limit to
    // satisfy the condition: serialized bytes' length exceeds the limit.
    byte[] preCompressedBytes = serialize(record);

    Assert.assertEquals(preCompressedBytes.length > limit, greaterThanThreshold);

    ZkSerializer zkSerializer = new ZNRecordSerializer();

    byte[] bytes;
    try {
      bytes = zkSerializer.serialize(record);

      Assert.assertEquals(bytes.length >= limit, exceptionExpected);
      Assert.assertFalse(exceptionExpected);
    } catch (HelixException ex) {
      Assert.assertTrue(exceptionExpected, "Should not throw ZkClientException.");
      Assert.assertTrue(ex.getMessage().contains(" is greater than " + limit + " bytes"));
      // No need to verify following asserts as bytes data is not returned.
      return;
    }

    // Verify whether serialized data is compressed or not.
    Assert.assertEquals(GZipCompressionUtil.isCompressed(bytes), compressionExpected);
    Assert.assertEquals(preCompressedBytes.length != bytes.length, compressionExpected);

    // Verify serialized bytes could correctly deserialize.
    Assert.assertEquals(zkSerializer.deserialize(bytes), record);
  }

  private ZNRecord createZNRecord(final int recordSizeKb) {
    byte[] buf = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      buf[i] = 'a';
    }
    String bufStr = new String(buf);

    ZNRecord record = new ZNRecord("record");
    for (int i = 0; i < recordSizeKb; i++) {
      record.setSimpleField(Integer.toString(i), bufStr);
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

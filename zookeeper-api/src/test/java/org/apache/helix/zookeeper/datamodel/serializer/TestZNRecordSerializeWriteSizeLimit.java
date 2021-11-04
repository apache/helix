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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.introspect.CodehausJacksonIntrospector;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
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
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED);
    final String compressionThresholdProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);

    try {
      // Prepare system properties to disable auto compression.
      final int writeSizeLimit = 200 * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      // 2. Set the auto compression enabled property to false so auto compression is disabled.
      System
          .setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED, "false");

      // Verify auto compression is disabled.
      Assert.assertFalse(
          Boolean.getBoolean(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED));
      // Data size 300 KB > size limit 200 KB: exception expected.
      verifyAutoCompression(300, false, true);

      // Data size 100 KB < size limit 200 KB: pass
      verifyAutoCompression(100, false, false);
    } finally {
      // Reset: add the properties back to system properties if they were originally available.
      if (compressionEnabledProperty != null) {
        System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED,
            compressionEnabledProperty);
      } else {
        System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED);
      }
      if (compressionThresholdProperty != null) {
        System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
            compressionThresholdProperty);
      } else {
        System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
      }
    }
  }

  /*
   * Tests data serializing when compress threshold is set.
   * Two cases:
   * 1. threshold is not set
   * --> default threshold (write size limit is used): pass or throw exception.
   * 2. threshold is set
   * --> serialized data is checked by the threshold.
   */
  @Test
  public void testZNRecordSerializerWriteSizeLimit() {
    // Backup properties for later resetting.
    final String writeSizeLimitProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
    final String compressThresholdProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES);

    try {
      // Unset write size limit and compress threshold properties so default limit is used.
      System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
      System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES);
      Assert.assertNull(
          System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES));
      Assert.assertNull(
          System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES));
      verifyAutoCompression(600, false, false);

      // 1. Set size limit but no compression threshold.
      final int writeSizeLimit = 200 * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));
      // Data size 100 KB < size limit 200 KB: directly write without compression.
      verifyAutoCompression(100, false, false);
      // Data size 300 KB > size limit 200 KB: compress expected.
      verifyAutoCompression(300, true, false);

      // 2. Set threshold so serialized compressed data even the size is smaller than the size limit.
      // Set it to 2000 bytes
      final int threshold = 100 * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES,
          String.valueOf(threshold));
      // Data size 150 KB < size limit 200 KB and > threshold 100 KB: compress expected.
      verifyAutoCompression(150, true, false);

      // 3. Set threshold to be larger than the write size limit. The default value will be used.
      int doubleLimitThreshold = writeSizeLimit * 2;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES,
          String.valueOf(doubleLimitThreshold));
      // Data size 250 KB > size limit 200 KB but < threshold 400 KB: compress is still expected.
      verifyAutoCompression(250, true, false);

      // 4. Set threshold to zero so compression will always be done.
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES,
          String.valueOf(0));
      // Data size 250 KB > size limit 200 KB but < threshold 400 KB: compress is still expected.
      verifyAutoCompression(5, true, false);
    } finally {
      // Reset: add the properties back to system properties if they were originally available.
      if (writeSizeLimitProperty != null) {
        System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
            writeSizeLimitProperty);
      } else {
        System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
      }
      if (compressThresholdProperty != null) {
        System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES,
            compressThresholdProperty);
      } else {
        System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES);
      }
    }
  }

  private void verifyAutoCompression(int recordSize, boolean compressionExpected, boolean exceptionExpected) {
    ZNRecord record = createZNRecord(recordSize);

    // Makes sure the length of serialized bytes is greater than limit to
    // satisfy the condition: serialized bytes' length exceeds the limit.
    byte[] preCompressedBytes = serialize(record);
    Assert.assertTrue(preCompressedBytes.length >= recordSize);

    ZkSerializer zkSerializer = new ZNRecordSerializer();

    byte[] bytes;
    try {
      bytes = zkSerializer.serialize(record);
      Assert.assertFalse(exceptionExpected);
    } catch (ZkMarshallingError e) {
      Assert.assertTrue(exceptionExpected, "Should not throw exception.");
      // No need to verify following asserts as bytes data is not returned.
      return;
    }

    // Verify whether serialized data is compressed or not.
    Assert.assertEquals(GZipCompressionUtil.isCompressed(bytes), compressionExpected);
    Assert.assertEquals(preCompressedBytes.length > bytes.length, compressionExpected,
        "Compressed data should have a smaller size compared with the original data.");

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
    mapper.setAnnotationIntrospector(new CodehausJacksonIntrospector());
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.enable(MapperFeature.AUTO_DETECT_FIELDS);
    mapper.enable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);

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

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

import java.util.Arrays;
import java.util.Date;

import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZNRecordSizeLimit extends ZkTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestZNRecordSizeLimit.class);

  private static final String ASSERTION_MESSAGE =
      "Should succeed because compressed data is smaller than 1M. Caused by: ";

  @Test
  public void testZNRecordSizeLimitUseZNRecordSerializer() {
    String className = getShortClassName();
    System.out.println("START testZNRecordSizeLimitUseZNRecordSerializer at " + new Date(
        System.currentTimeMillis()));

    ZNRecordSerializer serializer = new ZNRecordSerializer();

    String root = className;
    byte[] buf = new byte[1024];
    for (int i = 0; i < 1024; i++) {
      buf[i] = 'a';
    }
    String bufStr = new String(buf);

    // test zkClient
    // legal-sized data gets written to zk
    // write a znode of size less than 1m
    final ZNRecord smallRecord = new ZNRecord("normalsize");
    smallRecord.getSimpleFields().clear();
    for (int i = 0; i < 900; i++) {
      smallRecord.setSimpleField(i + "", bufStr);
    }

    String path1 = "/" + root + "/test1";
    _gZkClient.createPersistent(path1, true);
    _gZkClient.writeData(path1, smallRecord);

    ZNRecord record = _gZkClient.readData(path1);
    Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

    // oversized data doesn't create any data on zk
    // prepare a znode of size larger than 1m
    final ZNRecord largeRecord = new ZNRecord("oversize");
    largeRecord.getSimpleFields().clear();
    for (int i = 0; i < 1024; i++) {
      largeRecord.setSimpleField(i + "", bufStr);
    }
    String path2 = "/" + root + "/test2";
    _gZkClient.createPersistent(path2, true);
    try {
      _gZkClient.writeData(path2, largeRecord);
    } catch (ZkMarshallingError e) {
      Assert.fail(ASSERTION_MESSAGE + e);
    }
    record = _gZkClient.readData(path2);
    Assert.assertNotNull(record);

    // oversized write doesn't overwrite existing data on zk
    record = _gZkClient.readData(path1);
    try {
      _gZkClient.writeData(path1, largeRecord);
    } catch (ZkMarshallingError e) {
      Assert.fail(ASSERTION_MESSAGE + e);
    }
    ZNRecord recordNew = _gZkClient.readData(path1);
    try {
      byte[] arr = serializer.serialize(record);
      byte[] arrNew = serializer.serialize(recordNew);
      Assert.assertFalse(Arrays.equals(arr, arrNew));
    } catch (ZkMarshallingError e) {
      Assert.fail(ASSERTION_MESSAGE + e);
    }

    // test ZkDataAccessor
    ZKHelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.addCluster(className, true);
    InstanceConfig instanceConfig = new InstanceConfig("localhost_12918");
    admin.addInstance(className, instanceConfig);

    // oversized data should not create any new data on zk
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(className, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    IdealState idealState = new IdealState("currentState");
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    idealState.setNumPartitions(10);

    for (int i = 0; i < 1024; i++) {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    boolean succeed = accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);
    Assert.assertTrue(succeed);
    HelixProperty property = accessor.getProperty(
        keyBuilder.stateTransitionStatus("localhost_12918", "session_1", "partition_1"));
    Assert.assertNull(property);

    // legal sized data gets written to zk
    idealState.getRecord().getSimpleFields().clear();
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    idealState.setNumPartitions(10);

    for (int i = 0; i < 900; i++) {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    succeed = accessor.setProperty(keyBuilder.idealStates("TestDB1"), idealState);
    Assert.assertTrue(succeed);
    record = accessor.getProperty(keyBuilder.idealStates("TestDB1")).getRecord();
    try {
      Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);
    } catch (ZkMarshallingError e) {
      Assert.fail(ASSERTION_MESSAGE + e);
    }

    // oversized data should not update existing data on zk
    idealState.getRecord().getSimpleFields().clear();
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
    idealState.setNumPartitions(10);
    for (int i = 900; i < 1024; i++) {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    // System.out.println("record: " + idealState.getRecord());
    succeed = accessor.updateProperty(keyBuilder.idealStates("TestDB1"), idealState);
    Assert.assertTrue(succeed);
    recordNew = accessor.getProperty(keyBuilder.idealStates("TestDB1")).getRecord();
    try {
      byte[] arr = serializer.serialize(record);
      byte[] arrNew = serializer.serialize(recordNew);
      Assert.assertFalse(Arrays.equals(arr, arrNew));
    } catch (ZkMarshallingError e) {
      Assert.fail(ASSERTION_MESSAGE + e);
    }

    System.out.println("END testZNRecordSizeLimitUseZNRecordSerializer at " + new Date(
        System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testZNRecordSizeLimitUseZNRecordSerializer")
  public void testZNRecordSizeLimitUseZNRecordStreamingSerializer() {
    String className = getShortClassName();
    System.out.println("START testZNRecordSizeLimitUseZNRecordStreamingSerializer at " + new Date(
        System.currentTimeMillis()));

    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    HelixZkClient zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR));

    try {
      zkClient.setZkSerializer(serializer);
      String root = className;
      byte[] buf = new byte[1024];
      for (int i = 0; i < 1024; i++) {
        buf[i] = 'a';
      }
      String bufStr = new String(buf);

      // test zkClient
      // legal-sized data gets written to zk
      // write a znode of size less than 1m
      final ZNRecord smallRecord = new ZNRecord("normalsize");
      smallRecord.getSimpleFields().clear();
      for (int i = 0; i < 900; i++) {
        smallRecord.setSimpleField(i + "", bufStr);
      }

      String path1 = "/" + root + "/test1";
      zkClient.createPersistent(path1, true);
      zkClient.writeData(path1, smallRecord);

      ZNRecord record = zkClient.readData(path1);
      try {
        Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);
      } catch (ZkMarshallingError e) {
        Assert.fail(ASSERTION_MESSAGE + e);
      }

      // oversized data doesn't create any data on zk
      // prepare a znode of size larger than 1m
      final ZNRecord largeRecord = new ZNRecord("oversize");
      largeRecord.getSimpleFields().clear();
      for (int i = 0; i < 1024; i++) {
        largeRecord.setSimpleField(i + "", bufStr);
      }
      String path2 = "/" + root + "/test2";
      zkClient.createPersistent(path2, true);
      try {
        zkClient.writeData(path2, largeRecord);
      } catch (ZkMarshallingError e) {
        Assert.fail(ASSERTION_MESSAGE + e);
      }
      record = zkClient.readData(path2);
      Assert.assertNotNull(record);

      // oversized write doesn't overwrite existing data on zk
      record = zkClient.readData(path1);
      try {
        zkClient.writeData(path1, largeRecord);
      } catch (ZkMarshallingError e) {
        Assert.fail(ASSERTION_MESSAGE + e);
      }
      ZNRecord recordNew = zkClient.readData(path1);
      try {
        byte[] arr = serializer.serialize(record);
        byte[] arrNew = serializer.serialize(recordNew);
        Assert.assertFalse(Arrays.equals(arr, arrNew));
      } catch (ZkMarshallingError e) {
        Assert.fail(ASSERTION_MESSAGE + e);
      }

      // test ZkDataAccessor
      ZKHelixAdmin admin = new ZKHelixAdmin(zkClient);
      admin.addCluster(className, true);
      InstanceConfig instanceConfig = new InstanceConfig("localhost_12918");
      admin.addInstance(className, instanceConfig);

      // oversized data should not create any new data on zk
      ZKHelixDataAccessor accessor =
          new ZKHelixDataAccessor(className, new ZkBaseDataAccessor(zkClient));
      Builder keyBuilder = accessor.keyBuilder();

      // ZNRecord statusUpdates = new ZNRecord("statusUpdates");
      IdealState idealState = new IdealState("currentState");
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);

      for (int i = 0; i < 1024; i++) {
        idealState.getRecord().setSimpleField(i + "", bufStr);
      }
      boolean succeed = accessor.setProperty(keyBuilder.idealStates("TestDB_1"), idealState);
      Assert.assertTrue(succeed);
      HelixProperty property = accessor.getProperty(keyBuilder.idealStates("TestDB_1"));
      Assert.assertNotNull(property);

      // legal sized data gets written to zk
      idealState.getRecord().getSimpleFields().clear();
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);

      for (int i = 0; i < 900; i++) {
        idealState.getRecord().setSimpleField(i + "", bufStr);
      }
      succeed = accessor.setProperty(keyBuilder.idealStates("TestDB_2"), idealState);
      Assert.assertTrue(succeed);
      record = accessor.getProperty(keyBuilder.idealStates("TestDB_2")).getRecord();
      Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

      // oversized data should not update existing data on zk
      idealState.getRecord().getSimpleFields().clear();
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);

      for (int i = 900; i < 1024; i++) {
        idealState.getRecord().setSimpleField(i + "", bufStr);
      }
      // System.out.println("record: " + idealState.getRecord());
      succeed = accessor.updateProperty(keyBuilder.idealStates("TestDB_2"), idealState);
      Assert.assertTrue(succeed);
      recordNew = accessor.getProperty(keyBuilder.idealStates("TestDB_2")).getRecord();
      try {
        byte[] arr = serializer.serialize(record);
        byte[] arrNew = serializer.serialize(recordNew);
        Assert.assertFalse(Arrays.equals(arr, arrNew));
      } catch (ZkMarshallingError e) {
        Assert.fail(ASSERTION_MESSAGE + e);
      }
    } finally {
      zkClient.close();
    }

    System.out.println("END testZNRecordSizeLimitUseZNRecordStreamingSerializer at " + new Date(
        System.currentTimeMillis()));
  }

  /*
   * Tests ZNRecordSerializer threshold.
   * Two cases using ZkClient and ZkDataAccessor:
   * 1. serialized data size is less than threshold and could be written to ZK.
   * 2. serialized data size is greater than threshold, so ZkClientException is thrown.
   */
  @Test(dependsOnMethods = "testZNRecordSizeLimitUseZNRecordStreamingSerializer")
  public void testZNRecordSerializerWriteSizeLimit() throws Exception {
    // Backup properties for later resetting.
    final String thresholdProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);

    try {
      ZNRecordSerializer serializer = new ZNRecordSerializer();

      String root = getShortClassName();

      byte[] buf = new byte[1024];
      for (int i = 0; i < 1024; i++) {
        buf[i] = 'a';
      }
      String bufStr = new String(buf);

      // 1. legal-sized data gets written to zk
      // write a znode of size less than writeSizeLimit
      int rawZnRecordSize = 700;
      int writeSizeLimitKb = 800;
      int writeSizeLimit = writeSizeLimitKb * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      final ZNRecord normalSizeRecord = new ZNRecord("normal-size");
      for (int i = 0; i < rawZnRecordSize; i++) {
        normalSizeRecord.setSimpleField(Integer.toString(i), bufStr);
      }

      String path = "/" + root + "/normal";
      _gZkClient.createPersistent(path, true);
      _gZkClient.writeData(path, normalSizeRecord);

      ZNRecord record = _gZkClient.readData(path);

      // Successfully reads the same data.
      Assert.assertEquals(normalSizeRecord, record);

      int length = serializer.serialize(record).length;

      // Less than writeSizeLimit so it is written to ZK.
      Assert.assertTrue(length < writeSizeLimit);

      // 2. Large size data is not allowed to write to ZK
      // Set raw record size to be large enough so its serialized data exceeds the writeSizeLimit.
      rawZnRecordSize = 2000;
      // Set the writeSizeLimit to very small so serialized data size exceeds the writeSizeLimit.
      writeSizeLimitKb = 1;
      writeSizeLimit = writeSizeLimitKb * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      final ZNRecord largeRecord = new ZNRecord("large-size");
      for (int i = 0; i < rawZnRecordSize; i++) {
        largeRecord.setSimpleField(Integer.toString(i), bufStr);
      }

      path = "/" + root + "/large";
      _gZkClient.createPersistent(path, true);

      try {
        _gZkClient.writeData(path, largeRecord);
        Assert.fail("Data should not be written to ZK because data size exceeds writeSizeLimit!");
      } catch (ZkMarshallingError expected) {
        Assert.assertTrue(
            expected.getMessage().contains(" is greater than " + writeSizeLimit + " bytes"));
      }

      // test ZkDataAccessor
      ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);
      admin.addCluster(root, true);
      InstanceConfig instanceConfig = new InstanceConfig("localhost_12918");
      admin.addInstance(root, instanceConfig);

      // Set the writeSizeLimit to 10KB so serialized data size does not exceed writeSizeLimit.
      writeSizeLimitKb = 10;
      writeSizeLimit = writeSizeLimitKb * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      // oversized data should not create any new data on zk
      ZKHelixDataAccessor accessor =
          new ZKHelixDataAccessor(root, new ZkBaseDataAccessor<>(ZK_ADDR));
      Builder keyBuilder = accessor.keyBuilder();

      IdealState idealState = new IdealState("currentState");
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);

      for (int i = 0; i < 1024; i++) {
        idealState.getRecord().setSimpleField(Integer.toString(i), bufStr);
      }
      boolean succeed = accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);
      Assert.assertTrue(succeed);
      HelixProperty property = accessor.getProperty(
          keyBuilder.stateTransitionStatus("localhost_12918", "session_1", "partition_1"));
      Assert.assertNull(property);

      // legal sized data gets written to zk
      idealState.getRecord().getSimpleFields().clear();
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);

      for (int i = 0; i < 900; i++) {
        idealState.getRecord().setSimpleField(Integer.toString(i), bufStr);
      }
      succeed = accessor.setProperty(keyBuilder.idealStates("TestDB1"), idealState);
      Assert.assertTrue(succeed);
      record = accessor.getProperty(keyBuilder.idealStates("TestDB1")).getRecord();
      Assert.assertTrue(serializer.serialize(record).length < writeSizeLimit);

      // Set small write size limit so writing does not succeed.
      writeSizeLimitKb = 1;
      writeSizeLimit = writeSizeLimitKb * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      // oversized data should not update existing data on zk
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);
      for (int i = 900; i < 1024; i++) {
        idealState.getRecord().setSimpleField(Integer.toString(i), bufStr);
      }

      succeed = accessor.updateProperty(keyBuilder.idealStates("TestDB1"), idealState);
      Assert.assertFalse(succeed,
          "Update property should not succeed because data exceeds znode write limit!");

      // Delete the nodes.
      deletePath(_gZkClient, "/" + root);
    } finally {
      // Reset: add the properties back to system properties if they were originally available.
      if (thresholdProperty != null) {
        System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
            thresholdProperty);
      } else {
        System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
      }
    }
  }

  /*
   * Tests ZNRecordStreamingSerializer threshold.
   * Two cases using ZkClient and ZkDataAccessor:
   * 1. serialized data size is less than threshold and could be written to ZK.
   * 2. serialized data size is greater than threshold, so ZkClientException is thrown.
   */
  @Test(dependsOnMethods = "testZNRecordSerializerWriteSizeLimit")
  public void testZNRecordStreamingSerializerWriteSizeLimit() throws Exception {
    // Backup properties for later resetting.
    final String thresholdProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);

    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    HelixZkClient zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR));

    try {
      zkClient.setZkSerializer(serializer);

      String root = getShortClassName();

      byte[] buf = new byte[1024];
      for (int i = 0; i < 1024; i++) {
        buf[i] = 'a';
      }
      String bufStr = new String(buf);

      // 1. legal-sized data gets written to zk
      // write a znode of size less than writeSizeLimit
      int rawZnRecordSize = 700;
      int writeSizeLimitKb = 800;
      int writeSizeLimit = writeSizeLimitKb * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      final ZNRecord normalSizeRecord = new ZNRecord("normal-size");
      for (int i = 0; i < rawZnRecordSize; i++) {
        normalSizeRecord.setSimpleField(Integer.toString(i), bufStr);
      }

      String path = "/" + root + "/normal";
      zkClient.createPersistent(path, true);
      zkClient.writeData(path, normalSizeRecord);

      ZNRecord record = zkClient.readData(path);

      // Successfully reads the same data.
      Assert.assertEquals(normalSizeRecord, record);

      int length = serializer.serialize(record).length;

      // Less than writeSizeLimit so it is written to ZK.
      Assert.assertTrue(length < writeSizeLimit);

      // 2. Large size data is not allowed to write to ZK
      // Set raw record size to be large enough so its serialized data exceeds the writeSizeLimit.
      rawZnRecordSize = 2000;
      // Set the writeSizeLimit to very small so serialized data size exceeds the writeSizeLimit.
      writeSizeLimitKb = 1;
      writeSizeLimit = writeSizeLimitKb * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      final ZNRecord largeRecord = new ZNRecord("large-size");
      for (int i = 0; i < rawZnRecordSize; i++) {
        largeRecord.setSimpleField(Integer.toString(i), bufStr);
      }

      path = "/" + root + "/large";
      zkClient.createPersistent(path, true);

      try {
        zkClient.writeData(path, largeRecord);
        Assert.fail("Data should not written to ZK because data size exceeds writeSizeLimit!");
      } catch (ZkMarshallingError expected) {
        Assert.assertTrue(
            expected.getMessage().contains(" is greater than " + writeSizeLimit + " bytes"));
      }

      // test ZkDataAccessor
      ZKHelixAdmin admin = new ZKHelixAdmin(ZK_ADDR);
      admin.addCluster(root, true);
      InstanceConfig instanceConfig = new InstanceConfig("localhost_12918");
      admin.addInstance(root, instanceConfig);

      // Set the writeSizeLimit to 10KB so serialized data size does not exceed writeSizeLimit.
      writeSizeLimitKb = 10;
      writeSizeLimit = writeSizeLimitKb * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      // oversize data should not create any new data on zk
      ZKHelixDataAccessor accessor =
          new ZKHelixDataAccessor(root, new ZkBaseDataAccessor<>(ZK_ADDR));
      Builder keyBuilder = accessor.keyBuilder();

      IdealState idealState = new IdealState("currentState");
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);

      for (int i = 0; i < 1024; i++) {
        idealState.getRecord().setSimpleField(Integer.toString(i), bufStr);
      }
      boolean succeed = accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);
      Assert.assertTrue(succeed);
      HelixProperty property = accessor.getProperty(
          keyBuilder.stateTransitionStatus("localhost_12918", "session_1", "partition_1"));
      Assert.assertNull(property);

      // legal sized data gets written to zk
      idealState.getRecord().getSimpleFields().clear();
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);

      for (int i = 0; i < 900; i++) {
        idealState.getRecord().setSimpleField(Integer.toString(i), bufStr);
      }
      succeed = accessor.setProperty(keyBuilder.idealStates("TestDB1"), idealState);
      Assert.assertTrue(succeed);
      record = accessor.getProperty(keyBuilder.idealStates("TestDB1")).getRecord();
      Assert.assertTrue(serializer.serialize(record).length < writeSizeLimit);

      // Set small write size limit so writing does not succeed.
      writeSizeLimitKb = 1;
      writeSizeLimit = writeSizeLimitKb * 1024;
      System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
          String.valueOf(writeSizeLimit));

      // oversize data should not update existing data on zk
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(10);
      for (int i = 900; i < 1024; i++) {
        idealState.getRecord().setSimpleField(Integer.toString(i), bufStr);
      }

      succeed = accessor.updateProperty(keyBuilder.idealStates("TestDB1"), idealState);
      Assert.assertFalse(succeed,
          "Update property should not succeed because data exceeds znode write limit!");

      // Delete the nodes.
      deletePath(zkClient, "/" + root);
    } finally {
      zkClient.close();
      // Reset: add the properties back to system properties if they were originally available.
      if (thresholdProperty != null) {
        System.setProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES,
            thresholdProperty);
      } else {
        System.clearProperty(ZkSystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);
      }
    }
  }

  private void deletePath(final HelixZkClient zkClient, final String path) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> {
      do {
        try {
          zkClient.deleteRecursively(path);
        } catch (ZkClientException ex) {
          // ignore
        }
      } while (zkClient.exists(path));
      return true;
    }, TestHelper.WAIT_DURATION));
  }
}

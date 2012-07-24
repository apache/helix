/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.manager.zk;

import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;

public class TestZNRecordSizeLimit extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestZNRecordSizeLimit.class);

  @Test
  public void testZNRecordSizeLimitUseZNRecordSerializer()
  {
    String className = getShortClassName();
    System.out.println("START testZNRecordSizeLimitUseZNRecordSerializer at "
        + new Date(System.currentTimeMillis()));

    ZNRecordSerializer serializer = new ZNRecordSerializer();
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(serializer);
    String root = className;
    byte[] buf = new byte[1024];
    for (int i = 0; i < 1024; i++)
    {
      buf[i] = 'a';
    }
    String bufStr = new String(buf);

    // test zkClient
    // legal-sized data gets written to zk
    // write a znode of size less than 1m
    final ZNRecord smallRecord = new ZNRecord("normalsize");
    smallRecord.getSimpleFields().clear();
    for (int i = 0; i < 900; i++)
    {
      smallRecord.setSimpleField(i + "", bufStr);
    }

    String path1 = "/" + root + "/test1";
    zkClient.createPersistent(path1, true);
    zkClient.writeData(path1, smallRecord);

    ZNRecord record = zkClient.readData(path1);
    Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

    // oversized data doesn't create any data on zk
    // prepare a znode of size larger than 1m
    final ZNRecord largeRecord = new ZNRecord("oversize");
    largeRecord.getSimpleFields().clear();
    for (int i = 0; i < 1024; i++)
    {
      largeRecord.setSimpleField(i + "", bufStr);
    }
    String path2 = "/" + root + "/test2";
    zkClient.createPersistent(path2, true);
    try
    {
      zkClient.writeData(path2, largeRecord);
      Assert.fail("Should fail because data size is larger than 1M");
    }
    catch (HelixException e)
    {
      // OK
    }
    record = zkClient.readData(path2);
    Assert.assertNull(record);

    // oversized write doesn't overwrite existing data on zk
    record = zkClient.readData(path1);
    try
    {
      zkClient.writeData(path1, largeRecord);
      Assert.fail("Should fail because data size is larger than 1M");
    }
    catch (HelixException e)
    {
      // OK
    }
    ZNRecord recordNew = zkClient.readData(path1);
    byte[] arr = serializer.serialize(record);
    byte[] arrNew = serializer.serialize(recordNew);
    Assert.assertTrue(Arrays.equals(arr, arrNew));

    // test ZkDataAccessor
    ZKHelixAdmin admin = new ZKHelixAdmin(zkClient);
    admin.addCluster(className, true);
    InstanceConfig instanceConfig = new InstanceConfig("localhost_12918");
    admin.addInstance(className, instanceConfig);

    // oversized data should not create any new data on zk
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(className, new ZkBaseDataAccessor(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    IdealState idealState = new IdealState("currentState");
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setIdealStateMode("AUTO");
    idealState.setNumPartitions(10);

    for (int i = 0; i < 1024; i++)
    {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    boolean succeed = accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);
    Assert.assertFalse(succeed);
    HelixProperty property =
        accessor.getProperty(keyBuilder.stateTransitionStatus("localhost_12918",
                                                              "session_1",
                                                              "partition_1"));
    Assert.assertNull(property);

    // legal sized data gets written to zk
    idealState.getRecord().getSimpleFields().clear();
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setIdealStateMode("AUTO");
    idealState.setNumPartitions(10);

    for (int i = 0; i < 900; i++)
    {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    succeed = accessor.setProperty(keyBuilder.idealStates("TestDB1"), idealState);
    Assert.assertTrue(succeed);
    record =
        accessor.getProperty(keyBuilder.idealStates("TestDB1")).getRecord();
    Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

    // oversized data should not update existing data on zk
    idealState.getRecord().getSimpleFields().clear();
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setIdealStateMode("AUTO");
    idealState.setNumPartitions(10);
    for (int i = 900; i < 1024; i++)
    {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    // System.out.println("record: " + idealState.getRecord());
    succeed =
        accessor.updateProperty(keyBuilder.idealStates("TestDB1"), idealState);
    Assert.assertFalse(succeed);
    recordNew =
        accessor.getProperty(keyBuilder.idealStates("TestDB1")).getRecord();
    arr = serializer.serialize(record);
    arrNew = serializer.serialize(recordNew);
    Assert.assertTrue(Arrays.equals(arr, arrNew));

    System.out.println("END testZNRecordSizeLimitUseZNRecordSerializer at "
        + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testZNRecordSizeLimitUseZNRecordStreamingSerializer()
  {
    String className = getShortClassName();
    System.out.println("START testZNRecordSizeLimitUseZNRecordStreamingSerializer at "
        + new Date(System.currentTimeMillis()));

    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(serializer);
    String root = className;
    byte[] buf = new byte[1024];
    for (int i = 0; i < 1024; i++)
    {
      buf[i] = 'a';
    }
    String bufStr = new String(buf);

    // test zkClient
    // legal-sized data gets written to zk
    // write a znode of size less than 1m
    final ZNRecord smallRecord = new ZNRecord("normalsize");
    smallRecord.getSimpleFields().clear();
    for (int i = 0; i < 900; i++)
    {
      smallRecord.setSimpleField(i + "", bufStr);
    }

    String path1 = "/" + root + "/test1";
    zkClient.createPersistent(path1, true);
    zkClient.writeData(path1, smallRecord);

    ZNRecord record = zkClient.readData(path1);
    Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

    // oversized data doesn't create any data on zk
    // prepare a znode of size larger than 1m
    final ZNRecord largeRecord = new ZNRecord("oversize");
    largeRecord.getSimpleFields().clear();
    for (int i = 0; i < 1024; i++)
    {
      largeRecord.setSimpleField(i + "", bufStr);
    }
    String path2 = "/" + root + "/test2";
    zkClient.createPersistent(path2, true);
    try
    {
      zkClient.writeData(path2, largeRecord);
      Assert.fail("Should fail because data size is larger than 1M");
    }
    catch (HelixException e)
    {
      // OK
    }
    record = zkClient.readData(path2);
    Assert.assertNull(record);

    // oversized write doesn't overwrite existing data on zk
    record = zkClient.readData(path1);
    try
    {
      zkClient.writeData(path1, largeRecord);
      Assert.fail("Should fail because data size is larger than 1M");
    }
    catch (HelixException e)
    {
      // OK
    }
    ZNRecord recordNew = zkClient.readData(path1);
    byte[] arr = serializer.serialize(record);
    byte[] arrNew = serializer.serialize(recordNew);
    Assert.assertTrue(Arrays.equals(arr, arrNew));

    // test ZkDataAccessor
    ZKHelixAdmin admin = new ZKHelixAdmin(zkClient);
    admin.addCluster(className, true);
    InstanceConfig instanceConfig = new InstanceConfig("localhost_12918");
    admin.addInstance(className, instanceConfig);

    // oversized data should not create any new data on zk
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(className, new ZkBaseDataAccessor(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

//    ZNRecord statusUpdates = new ZNRecord("statusUpdates");
    IdealState idealState = new IdealState("currentState");
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setIdealStateMode("AUTO");
    idealState.setNumPartitions(10);

    for (int i = 0; i < 1024; i++)
    {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    boolean succeed =
        accessor.setProperty(keyBuilder.idealStates("TestDB_1"),
                                                              idealState);
    Assert.assertFalse(succeed);
    HelixProperty property =
        accessor.getProperty(keyBuilder.idealStates("TestDB_1"));
    Assert.assertNull(property);

    // legal sized data gets written to zk
    idealState.getRecord().getSimpleFields().clear();
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setIdealStateMode("AUTO");
    idealState.setNumPartitions(10);

    for (int i = 0; i < 900; i++)
    {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    succeed =
        accessor.setProperty(keyBuilder.idealStates("TestDB_2"),
                                                              idealState);
    Assert.assertTrue(succeed);
    record =
        accessor.getProperty(keyBuilder.idealStates("TestDB_2")).getRecord();
    Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

    // oversized data should not update existing data on zk
    idealState.getRecord().getSimpleFields().clear();
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setIdealStateMode("AUTO");
    idealState.setNumPartitions(10);

    for (int i = 900; i < 1024; i++)
    {
      idealState.getRecord().setSimpleField(i + "", bufStr);
    }
    // System.out.println("record: " + idealState.getRecord());
    succeed =
        accessor.updateProperty(keyBuilder.idealStates("TestDB_2"),
                                                                 idealState);
    Assert.assertFalse(succeed);
    recordNew =
        accessor.getProperty(keyBuilder.idealStates("TestDB_2")).getRecord();
    arr = serializer.serialize(record);
    arrNew = serializer.serialize(recordNew);
    Assert.assertTrue(Arrays.equals(arr, arrNew));

    System.out.println("END testZNRecordSizeLimitUseZNRecordStreamingSerializer at "
        + new Date(System.currentTimeMillis()));

  }
}

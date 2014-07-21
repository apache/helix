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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordUpdater;
import org.apache.helix.manager.zk.ZkBaseDataAccessor.AccessResult;
import org.apache.helix.manager.zk.ZkBaseDataAccessor.RetCode;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkBaseDataAccessor extends ZkTestBase {

  @Test
  public void testSyncSet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", testName, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");

    boolean success = _baseAccessor.set(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testSyncSetWithVersion() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", testName, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");

    // set persistent
    boolean success = _baseAccessor.set(path, record, 0, AccessOption.PERSISTENT);
    Assert.assertFalse(success, "Should fail since version not match");
    try {
      _zkclient.readData(path, false);
      Assert.fail("Should get no node exception");
    } catch (Exception e) {
      // OK
    }

    success = _baseAccessor.set(path, record, -1, AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    // set ephemeral
    path = String.format("/%s/%s", testName, "msg_1");
    record = new ZNRecord("msg_1");
    success = _baseAccessor.set(path, record, 0, AccessOption.EPHEMERAL);
    Assert.assertFalse(success);
    try {
      _zkclient.readData(path, false);
      Assert.fail("Should get no node exception");
    } catch (Exception e) {
      // OK
    }

    success = _baseAccessor.set(path, record, -1, AccessOption.EPHEMERAL);
    Assert.assertTrue(success);
    getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_1");

    record.setSimpleField("key0", "value0");
    success = _baseAccessor.set(path, record, 0, AccessOption.PERSISTENT);
    Assert.assertTrue(success, "Should pass. AccessOption.PERSISTENT is ignored");
    getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getSimpleFields().size(), 1);
    Assert.assertNotNull(getRecord.getSimpleField("key0"));
    Assert.assertEquals(getRecord.getSimpleField("key0"), "value0");

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncDoSet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s/%s", testName, "msg_0", "submsg_0");
    ZNRecord record = new ZNRecord("submsg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    AccessResult result = accessor.doSet(path, record, -1, AccessOption.PERSISTENT);
    Assert.assertEquals(result._retCode, RetCode.OK);
    Assert.assertEquals(result._pathCreated.size(), 3);
    Assert.assertTrue(result._pathCreated.contains(String.format("/%s", testName)));
    Assert.assertTrue(result._pathCreated.contains(String.format("/%s/%s", testName, "msg_0")));
    Assert.assertTrue(result._pathCreated.contains(path));

    Assert.assertTrue(_zkclient.exists(String.format("/%s", testName)));
    Assert.assertTrue(_zkclient.exists(String.format("/%s/%s", testName, "msg_0")));
    ZNRecord getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "submsg_0");

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncCreate() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", testName, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    boolean success = accessor.create(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    record.setSimpleField("key0", "value0");
    success = accessor.create(path, record, AccessOption.PERSISTENT);
    Assert.assertFalse(success, "Should fail since node already exists");
    getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getSimpleFields().size(), 0);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncUpdate() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", testName, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    boolean success = accessor.update(path, new ZNRecordUpdater(record), AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    record.setSimpleField("key0", "value0");
    success = accessor.update(path, new ZNRecordUpdater(record), AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getSimpleFields().size(), 1);
    Assert.assertNotNull(getRecord.getSimpleField("key0"));
    Assert.assertEquals(getRecord.getSimpleField("key0"), "value0");

    // test throw exception from updater
    success = accessor.update(path, new DataUpdater<ZNRecord>() {

      @Override
      public ZNRecord update(ZNRecord currentData) {
        throw new RuntimeException("IGNORABLE: test throw exception from updater");
      }
    }, AccessOption.PERSISTENT);
    Assert.assertFalse(success);
    getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getSimpleFields().size(), 1);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncRemove() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", testName, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    boolean success = accessor.remove(path, 0);
    Assert.assertFalse(success);

    success = accessor.create(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _zkclient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    success = accessor.remove(path, 0);
    Assert.assertTrue(success);
    Assert.assertFalse(_zkclient.exists(path));

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncGet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", testName, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    Stat stat = new Stat();
    ZNRecord getRecord = accessor.get(path, stat, 0);
    Assert.assertNull(getRecord);

    try {
      accessor.get(path, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);
      Assert.fail("Should throw exception if not exist");
    } catch (Exception e) {
      // OK
    }

    boolean success = accessor.create(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(success);

    getRecord = accessor.get(path, stat, 0);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");
    Assert.assertEquals(stat.getVersion(), 0);

    record.setSimpleField("key0", "value0");
    success = accessor.set(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(success);

    getRecord = accessor.get(path, stat, 0);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(record.getSimpleFields().size(), 1);
    Assert.assertNotNull(getRecord.getSimpleField("key0"));
    Assert.assertEquals(getRecord.getSimpleField("key0"), "value0");
    Assert.assertEquals(stat.getVersion(), 1);

    ZNRecord newRecord = new ZNRecord("msg_0");
    newRecord.setSimpleField("key1", "value1");
    success = accessor.update(path, new ZNRecordUpdater(newRecord), AccessOption.PERSISTENT);
    Assert.assertTrue(success);

    getRecord = accessor.get(path, stat, 0);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getSimpleFields().size(), 2);
    Assert.assertNotNull(getRecord.getSimpleField("key0"));
    Assert.assertEquals(getRecord.getSimpleField("key0"), "value0");
    Assert.assertNotNull(getRecord.getSimpleField("key1"));
    Assert.assertEquals(getRecord.getSimpleField("key1"), "value1");
    Assert.assertEquals(stat.getVersion(), 2);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncExist() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", testName, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    boolean success = accessor.exists(path, 0);
    Assert.assertFalse(success);

    success = accessor.create(path, record, AccessOption.EPHEMERAL);
    Assert.assertTrue(success);

    success = accessor.exists(path, 0);
    Assert.assertTrue(success);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testSyncGetStat() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", testName, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);

    Stat stat = accessor.getStat(path, 0);
    Assert.assertNull(stat);

    boolean success = accessor.create(path, record, AccessOption.EPHEMERAL);
    Assert.assertTrue(success);

    stat = accessor.getStat(path, 0);
    Assert.assertNotNull(stat);
    Assert.assertEquals(stat.getVersion(), 0);
    Assert.assertNotSame(stat.getEphemeralOwner(), 0);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testAsyncZkBaseDataAccessor() {
    System.out.println("START TestZkBaseDataAccessor.async at "
        + new Date(System.currentTimeMillis()));

    String root = "TestZkBaseDataAccessor_asyn";
    ZkClient zkClient = new ZkClient(_zkaddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.deleteRecursive("/" + root);

    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(zkClient);

    // test async createChildren
    String parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    List<String> paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
      records.add(new ZNRecord(msgId));
    }
    boolean[] success = accessor.createChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in create " + msgId);
    }

    // test get what we created
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getId(), msgId, "Should get what we created");
    }

    // test async setChildren
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    records = new ArrayList<ZNRecord>();
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key1", "value1");
      records.add(newRecord);
    }
    success = accessor.setChildren(paths, records, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in set " + msgId);
    }

    // test get what we set
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getSimpleFields().size(), 1, "Should have 1 simple field set");
      Assert.assertEquals(record.getSimpleField("key1"), "value1", "Should have value1 set");
    }

    // test async updateChildren
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    // records = new ArrayList<ZNRecord>();
    List<DataUpdater<ZNRecord>> znrecordUpdaters = new ArrayList<DataUpdater<ZNRecord>>();
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key2", "value2");
      // records.add(newRecord);
      znrecordUpdaters.add(new ZNRecordUpdater(newRecord));
    }
    success = accessor.updateChildren(paths, znrecordUpdaters, AccessOption.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in update " + msgId);
    }

    // test get what we updated
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getSimpleFields().size(), 2, "Should have 2 simple fields set");
      Assert.assertEquals(record.getSimpleField("key2"), "value2", "Should have value2 set");
    }

    // test async getChildren
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    records = accessor.getChildren(parentPath, null, 0);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      ZNRecord record = records.get(i);
      Assert.assertEquals(record.getId(), msgId, "Should get what we updated");
      Assert.assertEquals(record.getSimpleFields().size(), 2, "Should have 2 simple fields set");
      Assert.assertEquals(record.getSimpleField("key2"), "value2", "Should have value2 set");
    }

    // test async exists
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
    }
    boolean[] exists = accessor.exists(paths, 0);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertTrue(exists[i], "Should exist " + msgId);
    }

    // test async getStats
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
    }
    Stat[] stats = accessor.getStats(paths, 0);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertNotNull(stats[i], "Stat should exist for " + msgId);
      Assert.assertEquals(stats[i].getVersion(), 2,
          "DataVersion should be 2, since we set 1 and update 1 for " + msgId);
    }

    // test async remove
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
    }
    success = accessor.remove(paths, 0);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in remove " + msgId);
    }

    // test get what we removed
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId);
      boolean pathExists = zkClient.exists(path);
      Assert.assertFalse(pathExists, "Should be removed " + msgId);
    }

    zkClient.close();
    System.out.println("END TestZkBaseDataAccessor.async at "
        + new Date(System.currentTimeMillis()));
  }

}

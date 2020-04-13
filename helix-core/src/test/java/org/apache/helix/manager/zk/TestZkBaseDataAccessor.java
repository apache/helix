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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.ZNRecordUpdater;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZkBaseDataAccessor.AccessResult;
import org.apache.helix.manager.zk.ZkBaseDataAccessor.RetCode;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.helix.zookeeper.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.data.Stat;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestZkBaseDataAccessor extends ZkUnitTestBase {
  // serialize/deserialize integer list to byte array
  private static final ZkSerializer LIST_SERIALIZER = new ZkSerializer() {
    @Override
    public byte[] serialize(Object o)
        throws ZkMarshallingError {
      List<Integer> list = (List<Integer>) o;
      return list.stream().map(String::valueOf).collect(Collectors.joining(","))
          .getBytes();
    }

    @Override
    public Object deserialize(byte[] bytes)
        throws ZkMarshallingError {
      String string = new String(bytes);
      return Arrays.stream(string.split(",")).map(Integer::valueOf)
          .collect(Collectors.toList());
    }
  };
  String _rootPath = TestHelper.getTestClassName();


  @AfterMethod
  public void afterMethod() {
    String path = "/" + _rootPath;
    if (_gZkClient.exists(path)) {
      _gZkClient.deleteRecursively(path);
    }
  }

  @Test
  public void testSyncSet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", _rootPath, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    BaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    boolean success = accessor.set(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _gZkClient.readData(path);
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

    String path = String.format("/%s/%s", _rootPath, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    BaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    // set persistent
    boolean success = accessor.set(path, record, 0, AccessOption.PERSISTENT);
    Assert.assertFalse(success, "Should fail since version not match");
    try {
      _gZkClient.readData(path, false);
      Assert.fail("Should get no node exception");
    } catch (Exception e) {
      // OK
    }

    success = accessor.set(path, record, -1, AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _gZkClient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    // set ephemeral
    path = String.format("/%s/%s", _rootPath, "msg_1");
    record = new ZNRecord("msg_1");
    success = accessor.set(path, record, 0, AccessOption.EPHEMERAL);
    Assert.assertFalse(success);
    try {
      _gZkClient.readData(path, false);
      Assert.fail("Should get no node exception");
    } catch (Exception e) {
      // OK
    }

    success = accessor.set(path, record, -1, AccessOption.EPHEMERAL);
    Assert.assertTrue(success);
    getRecord = _gZkClient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_1");

    record.setSimpleField("key0", "value0");
    success = accessor.set(path, record, 0, AccessOption.PERSISTENT);
    Assert.assertTrue(success, "Should pass. AccessOption.PERSISTENT is ignored");
    getRecord = _gZkClient.readData(path);
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

    String path = String.format("/%s/%s/%s", _rootPath, "msg_0", "submsg_0");
    ZNRecord record = new ZNRecord("submsg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    AccessResult result = accessor.doSet(path, record, -1, AccessOption.PERSISTENT);
    Assert.assertEquals(result._retCode, RetCode.OK);
    Assert.assertEquals(result._pathCreated.size(), 3);
    Assert.assertTrue(result._pathCreated.contains(String.format("/%s/%s", _rootPath, "msg_0")));
    Assert.assertTrue(result._pathCreated.contains(path));

    Assert.assertTrue(_gZkClient.exists(String.format("/%s/%s", _rootPath, "msg_0")));
    ZNRecord getRecord = _gZkClient.readData(path);
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

    String path = String.format("/%s/%s", _rootPath, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<>(_gZkClient);

    boolean success = accessor.create(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _gZkClient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    record.setSimpleField("key0", "value0");
    success = accessor.create(path, record, AccessOption.PERSISTENT);
    Assert.assertFalse(success, "Should fail since node already exists");
    getRecord = _gZkClient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getSimpleFields().size(), 0);

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDefaultAccessorCreateCustomData() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", _rootPath, "msg_0");

    ZkBaseDataAccessor defaultAccessor = new ZkBaseDataAccessor(ZK_ADDR);

    List<Integer> l0 = ImmutableList.of(1, 2, 3);
    boolean createResult = defaultAccessor.create(path, l0, AccessOption.PERSISTENT);
    // The result is expected to be false because the list is not ZNRecord
    Assert.assertFalse(createResult);
    createResult = defaultAccessor.create(path, new ZNRecord("test"), AccessOption.PERSISTENT);
    // The result is expected to be true
    Assert.assertTrue(createResult);

    defaultAccessor.close();
    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCustomAccessorCreateZnRecord() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", _rootPath, "msg_0");

    ZkBaseDataAccessor customDataAccessor = new ZkBaseDataAccessor(ZK_ADDR, LIST_SERIALIZER);
    boolean createResult = customDataAccessor.create(path, new ZNRecord("test"), AccessOption.PERSISTENT);
    // The result is expected to be false because the ZnRecord is not List
    Assert.assertFalse(createResult);
    createResult = customDataAccessor.create(path, ImmutableList.of(1, 2, 3), AccessOption.PERSISTENT);
    // The result is expected to be true
    Assert.assertTrue(createResult);

    customDataAccessor.close();
    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncCreateWithCustomSerializer() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", _rootPath, "msg_0");

    ZkBaseDataAccessor<List<Integer>> accessor = new ZkBaseDataAccessor<>(ZK_ADDR, LIST_SERIALIZER);

    List<Integer> l0 = ImmutableList.of(1, 2, 3);
    List<Integer> l1 = ImmutableList.of(4, 5, 6);
    boolean createResult = accessor.create(path, l0, AccessOption.PERSISTENT);
    Assert.assertTrue(createResult);

    List<Integer> data = (List<Integer>) accessor.get(path, null, AccessOption.PERSISTENT);
    Assert.assertEquals(data, l0);
    boolean setResult = accessor.set(path, l1, 0, AccessOption.PERSISTENT);
    Assert.assertTrue(setResult);

    data = (List<Integer>) accessor.get(path, null, AccessOption.PERSISTENT);
    Assert.assertEquals(data, l1);

    accessor.close();
    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncUpdate() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", _rootPath, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    boolean success = accessor.update(path, new ZNRecordUpdater(record), AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _gZkClient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    record.setSimpleField("key0", "value0");
    success = accessor.update(path, new ZNRecordUpdater(record), AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    getRecord = _gZkClient.readData(path);
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
    getRecord = _gZkClient.readData(path);
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

    String path = String.format("/%s/%s", _rootPath, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

    // Base data accessor shall not fail when remove a non-exist path
    boolean success = accessor.remove(path, 0);
    Assert.assertTrue(success);

    success = accessor.create(path, record, AccessOption.PERSISTENT);
    Assert.assertTrue(success);
    ZNRecord getRecord = _gZkClient.readData(path);
    Assert.assertNotNull(getRecord);
    Assert.assertEquals(getRecord.getId(), "msg_0");

    // Tests that ZkClientException thrown from ZkClient should be caught
    // and remove() should return false.
    RealmAwareZkClient mockZkClient = Mockito.mock(RealmAwareZkClient.class);
    Mockito.doThrow(new ZkException("Failed to delete " + path)).when(mockZkClient)
        .delete(path);
    Mockito.doThrow(new ZkClientException("Failed to recursively delete " + path)).when(mockZkClient)
        .deleteRecursively(path);
    ZkBaseDataAccessor<ZNRecord> accessorMock =
        new ZkBaseDataAccessor<>(mockZkClient);
    try {
      Assert.assertFalse(accessorMock.remove(path, AccessOption.PERSISTENT),
          "Should return false because ZkClientException is thrown");
    } catch (ZkClientException e) {
      Assert.fail("Should not throw ZkClientException because it should be caught.");
    }

    success = accessor.remove(path, 0);
    Assert.assertTrue(success);
    Assert.assertFalse(_gZkClient.exists(path));

    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSyncGet() {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));

    String path = String.format("/%s/%s", _rootPath, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

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

    String path = String.format("/%s/%s", _rootPath, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

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

    String path = String.format("/%s/%s", _rootPath, "msg_0");
    ZNRecord record = new ZNRecord("msg_0");
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);

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
    System.out.println(
        "START TestZkBaseDataAccessor.async at " + new Date(System.currentTimeMillis()));

    String root = _rootPath;
    _gZkClient.deleteRecursively("/" + root);

    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<>(_gZkClient);

    // test async createChildren
    List<ZNRecord> records = new ArrayList<>();
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathBuilder.instanceMessage(root, "host_1", msgId));
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
      String path = PropertyPathBuilder.instanceMessage(root, "host_1", msgId);
      ZNRecord record = _gZkClient.readData(path);
      Assert.assertEquals(record.getId(), msgId, "Should get what we created");
    }

    // test async setChildren
    records = new ArrayList<>();
    paths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathBuilder.instanceMessage(root, "host_1", msgId));
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
      String path = PropertyPathBuilder.instanceMessage(root, "host_1", msgId);
      ZNRecord record = _gZkClient.readData(path);
      Assert.assertEquals(record.getSimpleFields().size(), 1, "Should have 1 simple field set");
      Assert.assertEquals(record.getSimpleField("key1"), "value1", "Should have value1 set");
    }

    // test async updateChildren
    // records = new ArrayList<ZNRecord>();
    List<DataUpdater<ZNRecord>> znrecordUpdaters = new ArrayList<DataUpdater<ZNRecord>>();
    paths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathBuilder.instanceMessage(root, "host_1", msgId));
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
      String path = PropertyPathBuilder.instanceMessage(root, "host_1", msgId);
      ZNRecord record = _gZkClient.readData(path);
      Assert.assertEquals(record.getSimpleFields().size(), 2, "Should have 2 simple fields set");
      Assert.assertEquals(record.getSimpleField("key2"), "value2", "Should have value2 set");
    }

    // test async getChildren
    String parentPath = PropertyPathBuilder.instanceMessage(root, "host_1");
    records = accessor.getChildren(parentPath, null, 0);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      ZNRecord record = records.get(i);
      Assert.assertEquals(record.getId(), msgId, "Should get what we updated");
      Assert.assertEquals(record.getSimpleFields().size(), 2, "Should have 2 simple fields set");
      Assert.assertEquals(record.getSimpleField("key2"), "value2", "Should have value2 set");
    }

    // test async exists
    paths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathBuilder.instanceMessage(root, "host_1", msgId));
    }
    boolean[] exists = accessor.exists(paths, 0);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertTrue(exists[i], "Should exist " + msgId);
    }

    // test async getStats
    paths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathBuilder.instanceMessage(root, "host_1", msgId));
    }
    Stat[] stats = accessor.getStats(paths, 0);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertNotNull(stats[i], "Stat should exist for " + msgId);
      Assert.assertEquals(stats[i].getVersion(), 2,
          "DataVersion should be 2, since we set 1 and update 1 for " + msgId);
    }

    // test async remove
    paths = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      paths.add(PropertyPathBuilder.instanceMessage(root, "host_1", msgId));
    }
    success = accessor.remove(paths, 0);
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in remove " + msgId);
    }

    // test get what we removed
    for (int i = 0; i < 10; i++) {
      String msgId = "msg_" + i;
      String path = PropertyPathBuilder.instanceMessage(root, "host_1", msgId);
      boolean pathExists = _gZkClient.exists(path);
      Assert.assertFalse(pathExists, "Should be removed " + msgId);
    }

    System.out.println("END TestZkBaseDataAccessor.async at "
        + new Date(System.currentTimeMillis()));
  }

}

package com.linkedin.helix.manager.zk;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.BaseDataAccessor.Option;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordUpdater;
import com.linkedin.helix.ZkUnitTestBase;

public class TestZkBaseDataAccessor extends ZkUnitTestBase
{
  @Test
  public void testSyncZkBaseDataAccessor()
  {
    System.out.println("START TestZkBaseDataAccessor.sync at " + new Date(System.currentTimeMillis()));

    String root = "TestZkBaseDataAccessor_syn";
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.deleteRecursive("/" + root);

    BaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(zkClient);

    // test sync create
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      boolean success = accessor.create(path, new ZNRecord(msgId), Option.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in create");
    }

    // test get what we created
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getId(), msgId, "Should get what we created");
    }

    // test sync set
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key1", "value1");
      boolean success = accessor.set(path, newRecord, Option.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in set");
    }

    // test get what we set
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getSimpleFields().size(), 1, "Should have 1 simple field set");
      Assert.assertEquals(record.getSimpleField("key1"), "value1", "Should have value1 set");
    }
    
    // test sync update
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key2", "value2");
      boolean success = accessor.update(path, new ZNRecordUpdater(newRecord), Option.PERSISTENT);
      Assert.assertTrue(success, "Should succeed in update");
    }

    // test get what we updated
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getSimpleFields().size(), 2, "Should have 2 simple fields set");
      Assert.assertEquals(record.getSimpleField("key2"), "value2", "Should have value2 set");
    }
    
    // test sync get
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      ZNRecord record = accessor.get(path, null, 0);
      Assert.assertEquals(record.getSimpleFields().size(), 2, "Should have 2 simple fields set");
      Assert.assertEquals(record.getSimpleField("key1"), "value1", "Should have value1 set");
      Assert.assertEquals(record.getSimpleField("key2"), "value2", "Should have value2 set");
    }

    // test sync exist
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      boolean exists = accessor.exists(path);
      Assert.assertTrue(exists, "Should exist");
    }

    // test getStat()
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      Stat stat = accessor.getStat(path);
      Assert.assertNotNull(stat, "Stat should exist");
      Assert.assertEquals(stat.getVersion(), 2, "DataVersion should be 2, since we set 1 and update 1");
    }

    // test sync remove
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      boolean success = accessor.remove(path);
      Assert.assertTrue(success, "Should remove");
    }
    
    // test get what we removed
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_0", msgId);
      boolean exists = zkClient.exists(path);
      Assert.assertFalse(exists, "Should be removed");
    }

    zkClient.close();
    System.out.println("END TestZkBaseDataAccessor.sync at " + new Date(System.currentTimeMillis()));
  }
  
  @Test
  public void testAsyncZkBaseDataAccessor()
  {
    System.out.println("START TestZkBaseDataAccessor.async at " + new Date(System.currentTimeMillis()));

    String root = "TestZkBaseDataAccessor_asyn";
    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.deleteRecursive("/" + root);

    BaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(zkClient);
    
    // test async createChildren
    String parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    List<ZNRecord> records = new ArrayList<ZNRecord>();
    List<String> paths = new  ArrayList<String>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1",msgId));
      records.add(new ZNRecord(msgId));
    }
    boolean[] success = accessor.createChildren(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in create " + msgId);
    }

    // test get what we created
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getId(), msgId, "Should get what we created");
    }

    // test async setChildren
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    records = new ArrayList<ZNRecord>();
    paths = new  ArrayList<String>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1",msgId));
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key1", "value1");
      records.add(newRecord);
    }
    success = accessor.setChildren(paths, records, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in set " + msgId);
    }

    // test get what we set
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getSimpleFields().size(), 1, "Should have 1 simple field set");
      Assert.assertEquals(record.getSimpleField("key1"), "value1", "Should have value1 set");
    }
    
    // test async updateChildren
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
//    records = new ArrayList<ZNRecord>();
    List<DataUpdater<ZNRecord>> znrecordUpdaters = new ArrayList<DataUpdater<ZNRecord>>();
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1",msgId));
      ZNRecord newRecord = new ZNRecord(msgId);
      newRecord.setSimpleField("key2", "value2");
//      records.add(newRecord);
      znrecordUpdaters.add(new ZNRecordUpdater(newRecord));
    }
    success = accessor.updateChildren(paths, znrecordUpdaters, Option.PERSISTENT);
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in update " + msgId);
    }

    // test get what we updated
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId);
      ZNRecord record = zkClient.readData(path);
      Assert.assertEquals(record.getSimpleFields().size(), 2, "Should have 2 simple fields set");
      Assert.assertEquals(record.getSimpleField("key2"), "value2", "Should have value2 set");
    }

    // test async getChildren
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    records = accessor.getChildren(parentPath, 0);
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      ZNRecord record = records.get(i);
      Assert.assertEquals(record.getId(), msgId, "Should get what we updated");
      Assert.assertEquals(record.getSimpleFields().size(), 2, "Should have 2 simple fields set");
      Assert.assertEquals(record.getSimpleField("key2"), "value2", "Should have value2 set");
    }

    // test async exists
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
    }
    boolean[] exists = accessor.exists(paths);
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      Assert.assertTrue(exists[i], "Should exist " + msgId);
    }

    // test async getStats
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
    }
    Stat[] stats = accessor.getStats(paths);
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      Assert.assertNotNull(stats[i], "Stat should exist for " + msgId);
      Assert.assertEquals(stats[i].getVersion(), 2, "DataVersion should be 2, since we set 1 and update 1 for " + msgId);
    }

    // test async remove
    parentPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1");
    paths = new ArrayList<String>();
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      paths.add(PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId));
    }
    success = accessor.remove(paths);
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      Assert.assertTrue(success[i], "Should succeed in remove " + msgId);
    }
    
    // test get what we removed
    for (int i = 0; i < 10; i++)
    {
      String msgId = "msg_" + i;
      String path = PropertyPathConfig.getPath(PropertyType.MESSAGES, root, "host_1", msgId);
      boolean pathExists = zkClient.exists(path);
      Assert.assertFalse(pathExists, "Should be removed " + msgId);
    }

    zkClient.close();
    System.out.println("END TestZkBaseDataAccessor.async at " + new Date(System.currentTimeMillis()));
  }
  
}

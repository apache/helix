package com.linkedin.helix.manager.zk;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.model.InstanceConfig;

public class TestZNRecordSizeLimit extends ZkUnitTestBase
{

  @Test
  public void testZNRecordSizeLimit()
  {
    String className = getShortClassName();
    System.out.println("START " + className + " at " + new Date(System.currentTimeMillis()));

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

    // test zkclient
    // write a znode of size less than 1m
    ZNRecord record = new ZNRecord("normalsize");
    record.getSimpleFields().clear();
    for (int i = 0; i < 900; i++)
    {
      record.setSimpleField(i + "", bufStr);
    }

    String path = "/" + root + "/test1";
    zkClient.createPersistent(path, true);
    zkClient.writeData(path, record);

    record = zkClient.readData(path);
    // System.out.println(serializer.serialize(record).length);
    Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

    // prepare a znode of size larger than 1m
    record = new ZNRecord("oversize");
    record.getSimpleFields().clear();
    for (int i = 0; i < 1024; i++)
    {
      record.setSimpleField(i + "", bufStr);
    }
    path = "/" + root + "/test2";
    zkClient.createPersistent(path, true);
    zkClient.writeData(path, record);
    record = zkClient.readData(path);
    System.out.println(serializer.serialize(record).length);
    Assert.assertNull(record);

    // test ZkDataAccessor
    ZKHelixAdmin admin = new ZKHelixAdmin(zkClient);
    admin.addCluster(className, true);
    InstanceConfig instanceConfig = new InstanceConfig("localhost_12918");
    admin.addInstance(className, instanceConfig);

    ZKDataAccessor accessor = new ZKDataAccessor(className, zkClient);
    ZNRecord statusUpdates = new ZNRecord("statusUpdates");

    for (int i = 0; i < 1024; i++)
    {
      statusUpdates.setSimpleField(i + "", bufStr);
    }
    accessor.setProperty(PropertyType.STATUSUPDATES, statusUpdates, "localhost_12918", "session_1", "partition_1");
    record = accessor.getProperty(PropertyType.STATUSUPDATES, "localhost_12918", "session_1", "partition_1");
    Assert.assertNull(record);

    statusUpdates.getSimpleFields().clear();
    for (int i = 0; i < 900; i++)
    {
      statusUpdates.setSimpleField(i + "", bufStr);
    }
    accessor.setProperty(PropertyType.STATUSUPDATES, statusUpdates, "localhost_12918", "session_1", "partition_2");
    record = accessor.getProperty(PropertyType.STATUSUPDATES, "localhost_12918", "session_1", "partition_2");
    Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

    statusUpdates.getSimpleFields().clear();
    for (int i = 900; i < 1024; i++)
    {
      statusUpdates.setSimpleField(i + "", bufStr);
    }
    // System.out.println("record: " + idealState.getRecord());
    accessor.updateProperty(PropertyType.STATUSUPDATES, statusUpdates, "localhost_12918", "session_1", "partition_2");
    record = accessor.getProperty(PropertyType.STATUSUPDATES, "localhost_12918", "session_1", "partition_2");
    Assert.assertNull(record);

    System.out.println("END " + className + " at " + new Date(System.currentTimeMillis()));

  }
}

package com.linkedin.helix.manager.zk;

import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.model.InstanceConfig;

public class TestZNRecordSizeLimit extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(TestZNRecordSizeLimit.class);

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
    } catch (HelixException e)
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
    } catch (HelixException e)
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
    ZKDataAccessor accessor = new ZKDataAccessor(className, zkClient);
    ZNRecord statusUpdates = new ZNRecord("statusUpdates");

    for (int i = 0; i < 1024; i++)
    {
      statusUpdates.setSimpleField(i + "", bufStr);
    }
    boolean succeed = accessor.setProperty(PropertyType.STATUSUPDATES, statusUpdates, "localhost_12918", "session_1", "partition_1");
    Assert.assertFalse(succeed);
    record = accessor.getProperty(PropertyType.STATUSUPDATES, "localhost_12918", "session_1", "partition_1");
    Assert.assertNull(record);

    // legal sized data gets written to zk
    statusUpdates.getSimpleFields().clear();
    for (int i = 0; i < 900; i++)
    {
      statusUpdates.setSimpleField(i + "", bufStr);
    }
    succeed = accessor.setProperty(PropertyType.STATUSUPDATES, statusUpdates, "localhost_12918", "session_1", "partition_2");
    Assert.assertTrue(succeed);
    record = accessor.getProperty(PropertyType.STATUSUPDATES, "localhost_12918", "session_1", "partition_2");
    Assert.assertTrue(serializer.serialize(record).length > 900 * 1024);

    // oversized data should not update existing data on zk
    statusUpdates.getSimpleFields().clear();
    for (int i = 900; i < 1024; i++)
    {
      statusUpdates.setSimpleField(i + "", bufStr);
    }
    // System.out.println("record: " + idealState.getRecord());
    succeed = accessor.updateProperty(PropertyType.STATUSUPDATES, statusUpdates, "localhost_12918", "session_1", "partition_2");
    Assert.assertTrue(succeed);
    recordNew = accessor.getProperty(PropertyType.STATUSUPDATES, "localhost_12918", "session_1", "partition_2");
    arr = serializer.serialize(record);
    arrNew = serializer.serialize(recordNew);
    Assert.assertTrue(Arrays.equals(arr, arrNew));

    System.out.println("END " + className + " at " + new Date(System.currentTimeMillis()));

  }
}

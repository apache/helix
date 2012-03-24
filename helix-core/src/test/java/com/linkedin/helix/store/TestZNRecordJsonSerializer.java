package com.linkedin.helix.store;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.zk.ZKPropertyStore;

public class TestZNRecordJsonSerializer extends ZkUnitTestBase
{

  @Test
  public void testZNRecordJsonSerializer() throws Exception
  {
    final String testRoot = getShortClassName();

    System.out.println("START " + testRoot + " at " + new Date(System.currentTimeMillis()));

    ZNRecord record = new ZNRecord("node1");
    record.setSimpleField(ZNRecord.LIST_FIELD_BOUND, "" + 3);
    List<String> list = Arrays.asList("one", "two", "three", "four");
    record.setListField("list1", list);

    ZKPropertyStore<ZNRecord> store = new ZKPropertyStore<ZNRecord>(new ZkClient(ZK_ADDR),
        new ZNRecordJsonSerializer(), "/" + testRoot);

    store.setProperty("node1", record);
    ZNRecord newRecord = store.getProperty("node1");
    Assert.assertTrue(newRecord.getListField("list1").size() == 3);

    System.out.println("START " + testRoot + " at " + new Date(System.currentTimeMillis()));

  }
}

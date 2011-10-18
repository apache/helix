package com.linkedin.clustermanager;

import java.io.FileFilter;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.HierarchicalDataHolder;

public class TestHierarchicalDataStore extends ZkUnitTestBase
{
  protected static ZkClient _zkClientString = null;

  @Test (groups = {"unitTest"})
  public void testHierarchicalDataStore()
  {
    _zkClientString = new ZkClient(ZK_ADDR, 1000, 3000);

    String path = "/tmp/testHierarchicalDataStore";
    FileFilter filter = null;
    // _zkClient.setZkSerializer(new ZNRecordSerializer());

    _zkClientString.deleteRecursive(path);
    HierarchicalDataHolder<ZNRecord> dataHolder = new HierarchicalDataHolder<ZNRecord>(
        _zkClientString, path, filter);
    dataHolder.print();
    Assert.assertFalse(dataHolder.refreshData());

    // write data
    add(path, "root data");
    Assert.assertTrue(dataHolder.refreshData());
    dataHolder.print();

    // add some children
    add(path + "/child1", "child 1 data");
    add(path + "/child2", "child 2 data");
    Assert.assertTrue(dataHolder.refreshData());
    dataHolder.print();

    // add some grandchildren
    add(path + "/child1" + "/grandchild1", "grand child 1 data");
    add(path + "/child1" + "/grandchild2", "grand child 2 data");
    Assert.assertTrue(dataHolder.refreshData());
    dataHolder.print();
    
    Assert.assertFalse(dataHolder.refreshData());
    
    set(path + "/child1", "new child 1 data");
    Assert.assertTrue(dataHolder.refreshData());
    dataHolder.print();
  }

  private void set(String path, String data)
  {
    _zkClientString.writeData(path, data);
  }

  private void add(String path, String data)
  {
    _zkClientString.createPersistent(path, true);
    _zkClientString.writeData(path, data);
  }

}

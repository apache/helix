package com.linkedin.clustermanager.agent.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZkUnitTestBase;
import com.linkedin.clustermanager.model.IdealState;

public class TestZKUtil extends ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(ZkUnitTestBase.class);

  String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  ZkClient _zkClient;

  @BeforeClass()
  public void beforeClass() throws IOException, Exception
  {
  	_zkClient = new ZkClient(ZK_ADDR);
  	_zkClient.setZkSerializer(new ZNRecordSerializer());
    if (_zkClient.exists("/" + clusterName))
    {
      _zkClient.deleteRecursive("/" + clusterName);
    }

    boolean result = ZKUtil.isClusterSetup(clusterName, _zkClient);
    AssertJUnit.assertFalse(result);
    result = ZKUtil.isClusterSetup(null, _zkClient);
    AssertJUnit.assertFalse(result);

    result = ZKUtil.isClusterSetup(null, null);
    AssertJUnit.assertFalse(result);

    result = ZKUtil.isClusterSetup(clusterName, null);
    AssertJUnit.assertFalse(result);

    TestHelper.setupEmptyCluster(_zkClient, clusterName);
  }

  @AfterClass()
  public void afterClass()
  {
  	_zkClient.close();
  }

  @Test()
  public void testIsClusterSetup()
  {
    boolean result = ZKUtil.isClusterSetup(clusterName, _zkClient);
    AssertJUnit.assertTrue(result);
  }

  @Test()
  public void testChildrenOperations()
  {
    List<ZNRecord> list = new ArrayList<ZNRecord>();
    list.add(new ZNRecord("id1"));
    list.add(new ZNRecord("id2"));
    String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName);
    ZKUtil.createChildren(_zkClient, path, list);
    list = ZKUtil.getChildren(_zkClient, path);
    AssertJUnit.assertEquals(2, list.size());

    ZKUtil.dropChildren(_zkClient, path, list);
    ZKUtil.dropChildren(_zkClient, path, new ZNRecord("id1"));
    list = ZKUtil.getChildren(_zkClient, path);
    AssertJUnit.assertEquals(0, list.size());

    ZKUtil.dropChildren(_zkClient, path, (List<ZNRecord>) null);
  }

  @Test()
  public void testUpdateIfExists()
  {
    String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, "id3");
    ZNRecord record = new ZNRecord("id4");
    ZKUtil.updateIfExists(_zkClient, path, record, false);
    AssertJUnit.assertFalse(_zkClient.exists(path));
    _zkClient.createPersistent(path);
    ZKUtil.updateIfExists(_zkClient, path, record, false);
    AssertJUnit.assertTrue(_zkClient.exists(path));
    record = _zkClient.<ZNRecord>readData(path);
    AssertJUnit.assertEquals("id4", record.getId());
  }

  @Test()
  public void testSubstract()
  {
    String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, "id5");
    ZNRecord record = new ZNRecord("id5");
    record.setSimpleField("key1", "value1");
    _zkClient.createPersistent(path, record);
    ZKUtil.substract(_zkClient, path, record);
    record = _zkClient.<ZNRecord>readData(path);
    AssertJUnit.assertNull(record.getSimpleField("key1"));
  }

  @Test()
  public void testNullChildren()
  {
    String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, "id6");
    ZKUtil.createChildren(_zkClient, path, (List<ZNRecord>) null);
  }

  @Test()
  public void testCreateOrUpdate()
  {
    String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, "id7");
    ZNRecord record = new ZNRecord("id7");
    ZKUtil.createOrUpdate(_zkClient, path, record, true, true);
    record = _zkClient.<ZNRecord>readData(path);
    AssertJUnit.assertEquals("id7", record.getId());
  }

  @Test()
  public void testCreateOrReplace()
  {
    String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, "id8");
    ZNRecord record = new ZNRecord("id8");
    ZKUtil.createOrReplace(_zkClient, path, record, true);
    record = _zkClient.<ZNRecord>readData(path);
    AssertJUnit.assertEquals("id8", record.getId());
    record = new ZNRecord("id9");
    ZKUtil.createOrReplace(_zkClient, path, record, true);
    record = _zkClient.<ZNRecord>readData(path);
    AssertJUnit.assertEquals("id9", record.getId());
  }

  @Test()
  public void testGetChildsIfDataChanged()
  {
    String idealStatPath = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName);
    _zkClient.createPersistent(idealStatPath + "/is1", new ZNRecord("is1"));
    ZNRecord record2 = new ZNRecord("is2");
    _zkClient.createPersistent(idealStatPath + "/is2", record2);

    Map<String, IdealState> records = new HashMap<String, IdealState>();
    ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);

    // do one more read, since it seems zk update stat even though no data change for the first two reads
    ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);
    LOG.debug("records:" + records);
    Assert.assertEquals(records.get("is1").getRecord().getId(), "is1");
    Assert.assertEquals(records.get("is2").getRecord().getId(), "is2");

    // no data change
    boolean dataChanged = ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);
    LOG.debug("records:" + records);
    Assert.assertFalse(dataChanged, "Stat should not change since no data change");

    // change value of an existing znode
    record2.setSimpleField("key1", "value1");
    _zkClient.writeData(idealStatPath + "/is2", record2);
    dataChanged = ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);
    LOG.debug("records:" + records);
    Assert.assertEquals(records.get("is2").getRecord().getSimpleField("key1"), "value1");
    Assert.assertTrue(dataChanged, "Stat should change, since data changed");

    dataChanged = ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);
    LOG.debug("records:" + records);
    Assert.assertFalse(dataChanged, "Stat should not change, since no data change");

    // add a new child
    _zkClient.createPersistent(idealStatPath + "/is3", new ZNRecord("is3"));
    dataChanged = ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);
    LOG.debug("records:" + records);
    Assert.assertEquals(records.get("is3").getRecord().getId(), "is3");
    Assert.assertTrue(dataChanged, "Stat should change, since new node added");

    dataChanged = ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);
    LOG.debug("records:" + records);
    Assert.assertFalse(dataChanged, "Stat should not change, since no data change");

    // delete a child
    _zkClient.delete(idealStatPath + "/is2");
    dataChanged = ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);
    LOG.debug("records:" + records);
    Assert.assertNull(records.get("is2"), "Should be null, since is2 has been deleted");
    Assert.assertTrue(dataChanged, "Stat should change, since a node is deleted");

    dataChanged = ZKUtil.<IdealState>getChildsIfDataChanged(_zkClient, idealStatPath, records, IdealState.class);
    LOG.debug("records:" + records);
    Assert.assertFalse(dataChanged, "Stat should not change, since no data change");
  }
}

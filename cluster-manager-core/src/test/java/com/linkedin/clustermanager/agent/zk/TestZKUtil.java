package com.linkedin.clustermanager.agent.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZkUnitTestBase;

public class TestZKUtil extends ZkUnitTestBase
{
  String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  
  @BeforeClass(groups = { "unitTest" })
  public void setup() throws IOException, Exception
  {
    if (_zkClient.exists("/" + clusterName))
    {
      _zkClient.deleteRecursive("/" + clusterName);
    }
    
    boolean result = ZKUtil.isClusterSetup(clusterName, _zkClient);
    AssertJUnit.assertFalse(result);
    TestHelper.setupEmptyCluster(_zkClient, clusterName);
  }

  @Test(groups = { "unitTest" })
  public void testIsClusterSetup()
  {
    boolean result = ZKUtil.isClusterSetup(clusterName, _zkClient);
    AssertJUnit.assertTrue(result);
  }
  
  @Test(groups = { "unitTest" })
  public void testChildrenOperations()
  {
    List<ZNRecord> list = new ArrayList<ZNRecord>();
    list.add(new ZNRecord("id1"));
    list.add(new ZNRecord("id2"));
    String path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName);
    ZKUtil.createChildren(_zkClient, path, list);
    list = ZKUtil.getChildren(_zkClient, path);
    AssertJUnit.assertEquals(2, list.size());
    
    ZKUtil.dropChildren(_zkClient, path, new ZNRecord("id1"));
    list = ZKUtil.getChildren(_zkClient, path);
    AssertJUnit.assertEquals(1, list.size());
  }
  
  @Test(groups = { "unitTest" })
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
  
  @Test(groups = { "unitTest" })
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
}

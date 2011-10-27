package com.linkedin.clustermanager.agent.zk;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.zookeeper.data.Stat;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyPathConfig;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZkUnitTestBase;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.store.PropertyStoreException;


public class TestZKDataAccessor extends ZkUnitTestBase
{
	
  private ClusterDataAccessor _accessor;
  private String _clusterName; 
  private final String resourceGroup = "resourceGroup";

	ZkClient _zkClient;
	
  @Test (groups = { "unitTest" })
  public void testSet()
  {
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    boolean success = _accessor.setProperty(PropertyType.IDEALSTATES, record, resourceGroup);
    AssertJUnit.assertTrue(success);
    // String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    AssertJUnit.assertTrue(_zkClient.exists(path));
    AssertJUnit.assertEquals(record ,_zkClient.readData(path));
        
    record.setSimpleField("partitions", "20");
    success = _accessor.setProperty(PropertyType.IDEALSTATES, record, resourceGroup);
    AssertJUnit.assertTrue(success);
    AssertJUnit.assertTrue(_zkClient.exists(path));
    AssertJUnit.assertEquals(record ,_zkClient.readData(path));
    
  }
  
  @Test (groups = { "unitTest" })
  public void testGet()
  {
    // String resourceGroup = "resourceGroup";
    // String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, record);
    ZNRecord value = _accessor.getProperty(PropertyType.IDEALSTATES, resourceGroup);
    AssertJUnit.assertNotNull(value);
    AssertJUnit.assertEquals(record, value);
  }
 
  @Test (groups = { "unitTest" })
  public void testRemove()
  {
    // String resourceGroup = "resourceGroup";
    // String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, record);
    boolean success = _accessor.removeProperty(PropertyType.IDEALSTATES, resourceGroup);
    AssertJUnit.assertTrue(success);
    AssertJUnit.assertFalse(_zkClient.exists(path));
    ZNRecord value = _accessor.getProperty(PropertyType.IDEALSTATES, resourceGroup);
    AssertJUnit.assertNull(value);

  }
  
  @Test (groups = { "unitTest" })
  public void testUpdate()
  {
    // String resourceGroup = "resourceGroup";
    // String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, record);
    Stat stat = _zkClient.getStat(path);
    
    record.setSimpleField("testField", "newValue");
    boolean success = _accessor.updateProperty(PropertyType.IDEALSTATES, record,resourceGroup);
    AssertJUnit.assertTrue(success);
    AssertJUnit.assertTrue(_zkClient.exists(path));
    ZNRecord value = _zkClient.readData(path);
    AssertJUnit.assertEquals(record,value);
    Stat newstat = _zkClient.getStat(path);
    
    AssertJUnit.assertEquals(stat.getCtime(), newstat.getCtime());
    AssertJUnit.assertNotSame(stat.getMtime(), newstat.getMtime());
    AssertJUnit.assertTrue(stat.getMtime() < newstat.getMtime());
  }

  @Test (groups = { "unitTest" })
  public void testGetChildValues()
  {
    List<ZNRecord> list = _accessor.getChildValues(PropertyType.EXTERNALVIEW, _clusterName);
    AssertJUnit.assertEquals(0, list.size());
  }
  
  @Test (groups = { "unitTest" })
  public void testGetPropertyStore()
  {
    PropertyStore<ZNRecord> store = _accessor.getStore();
    try
    {
      store.setProperty("child1", new ZNRecord("child1"));
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  // START STANDARD STUFF TO START ZK SERVER
  // private String _zkServerAddress;
  // private List<ZkServer> _localZkServers;
  // private ZkClient _zkClient;
  
  @BeforeClass
  public void beforeClass() throws IOException, Exception
  {
    // List<Integer> localPorts = new ArrayList<Integer>();
    // localPorts.add(2300);
    // localPorts.add(2301);

    // _localZkServers = startLocalZookeeper(localPorts,
    //    System.getProperty("user.dir") + "/" + "zkdata", 2000);
    // _zkServerAddress = "localhost:" + 2301;

    // _zkClient = new ZkClient(_zkServerAddress);
    // _zkClient.setZkSerializer(new ZNRecordSerializer());
    _clusterName = CLUSTER_PREFIX + "_" + getShortClassName();  // testCluster";
    
		System.out.println("START TestZKDataAccessor.beforeClass() at " + new Date(System.currentTimeMillis()));
		_zkClient = new ZkClient(ZK_ADDR);
		_zkClient.setZkSerializer(new ZNRecordSerializer());
    
    if (_zkClient.exists("/" + _clusterName))
    {
      _zkClient.deleteRecursive("/" + _clusterName);
    }
    _accessor = new ZKDataAccessor(_clusterName, _zkClient);
  }

  @AfterClass
  public void afterClass()
  {
		_zkClient.close();
		System.out.println("END TestZKDataAccessor.beforeClass() at " + new Date(System.currentTimeMillis()));
  }
  
}

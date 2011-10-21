package com.linkedin.clustermanager.agent.zk;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
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

  @Test (groups = { "unitTest" })
  public void testSet()
  {
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    boolean success = _accessor.setProperty(PropertyType.IDEALSTATES, record, resourceGroup);
    Assert.assertTrue(success);
    // String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    String path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, _clusterName, resourceGroup);
    Assert.assertTrue(_zkClient.exists(path));
    Assert.assertEquals(record ,_zkClient.readData(path));
        
    record.setSimpleField("partitions", "20");
    success = _accessor.setProperty(PropertyType.IDEALSTATES, record, resourceGroup);
    Assert.assertTrue(success);
    Assert.assertTrue(_zkClient.exists(path));
    Assert.assertEquals(record ,_zkClient.readData(path));
    
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
    Assert.assertNotNull(value);
    Assert.assertEquals(record, value);
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
    Assert.assertTrue(success);
    Assert.assertFalse(_zkClient.exists(path));
    ZNRecord value = _accessor.getProperty(PropertyType.IDEALSTATES, resourceGroup);
    Assert.assertNull(value);

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
    Assert.assertTrue(success);
    Assert.assertTrue(_zkClient.exists(path));
    ZNRecord value = _zkClient.readData(path);
    Assert.assertEquals(record,value);
    Stat newstat = _zkClient.getStat(path);
    
    Assert.assertEquals(stat.getCtime(), newstat.getCtime());
    Assert.assertNotSame(stat.getMtime(), newstat.getMtime());
    Assert.assertTrue(stat.getMtime() < newstat.getMtime());
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


  /*
  public static List<ZkServer> startLocalZookeeper(
      List<Integer> localPortsList, String zkTestDataRootDir, int tickTime)
      throws IOException
  {
    List<ZkServer> localZkServers = new ArrayList<ZkServer>();

    int count = 0;
    for (int port : localPortsList)
    {
      ZkServer zkServer = startZkServer(zkTestDataRootDir, count++, port,
          tickTime);
      localZkServers.add(zkServer);
    }
    return localZkServers;
  }

  public static ZkServer startZkServer(String zkTestDataRootDir, int machineId,
      int port, int tickTime) throws IOException
  {
    File zkTestDataRootDirFile = new File(zkTestDataRootDir);
    zkTestDataRootDirFile.mkdirs();

    String dataPath = zkTestDataRootDir + "/" + machineId + "/" + port
        + "/data";
    String logPath = zkTestDataRootDir + "/" + machineId + "/" + port + "/log";

    FileUtils.deleteDirectory(new File(dataPath));
    FileUtils.deleteDirectory(new File(logPath));

    IDefaultNameSpace mockDefaultNameSpace = new IDefaultNameSpace()
    {

      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
      }
    };

    ZkServer zkServer = new ZkServer(dataPath, logPath, mockDefaultNameSpace,
        port, tickTime);
    zkServer.start();

    return zkServer;
  }

  private static void stopLocalZookeeper(List<ZkServer> localZkServers)
  {
    for (ZkServer zkServer : localZkServers)
    {
      zkServer.shutdown();
    }
  }
  */
  
  @BeforeClass(groups = { "unitTest" })
  public void setup() throws IOException, Exception
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
    
    if (_zkClient.exists("/" + _clusterName))
    {
      _zkClient.deleteRecursive("/" + _clusterName);
    }
    _accessor = new ZKDataAccessor(_clusterName, _zkClient);
  }

  @AfterClass(groups = { "unitTest" })
  public void tearDown()
  {
    // stopLocalZookeeper(_localZkServers);
  }

}

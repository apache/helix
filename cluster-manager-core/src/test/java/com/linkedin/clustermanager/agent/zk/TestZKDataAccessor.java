package com.linkedin.clustermanager.agent.zk;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;

// TODO inherit from ZkTestBase
@Test
public class TestZKDataAccessor
{

  public void testSet()
  {
    String resourceGroup = "resourceGroup";
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    boolean success = _accessor.setProperty(PropertyType.IDEALSTATES, record, resourceGroup);
    Assert.assertTrue(success);
    String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    Assert.assertTrue(_zkClient.exists(path));
    Assert.assertEquals(record ,_zkClient.readData(path));
        
    record.setSimpleField("partitions", "20");
    success = _accessor.setProperty(PropertyType.IDEALSTATES, record, resourceGroup);
    Assert.assertTrue(success);
    Assert.assertTrue(_zkClient.exists(path));
    Assert.assertEquals(record ,_zkClient.readData(path));
    
  }
  
  public void testGet(){
    String resourceGroup = "resourceGroup";
    String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, record);
    ZNRecord value = _accessor.getProperty(PropertyType.IDEALSTATES,resourceGroup);
    Assert.assertNotNull(value);
    Assert.assertEquals(record,value);
  }
 
  public void testRemove(){
    String resourceGroup = "resourceGroup";
    String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, record);
    boolean success = _accessor.removeProperty(PropertyType.IDEALSTATES,resourceGroup);
    Assert.assertTrue(success);
    Assert.assertFalse(_zkClient.exists(path));
    ZNRecord value = _accessor.getProperty(PropertyType.IDEALSTATES,resourceGroup);
    Assert.assertNull(value);

  }
  public void testUpdate(){
    String resourceGroup = "resourceGroup";
    String path = "/"+_clusterName +"/IDEALSTATES/"+resourceGroup;
    ZNRecord record = new ZNRecord(resourceGroup);
    record.setSimpleField("testField", "testValue");
    _zkClient.delete(path);
    _zkClient.createPersistent(new File(path).getParent(), true);
    _zkClient.createPersistent(path, record);
    Stat stat = _zkClient.getStat(path);
    
    record.setSimpleField("testField", "newValue");
    boolean success = _accessor.updateProperty(PropertyType.IDEALSTATES,record,resourceGroup);
    Assert.assertTrue(success);
    Assert.assertTrue(_zkClient.exists(path));
    ZNRecord value = _zkClient.readData(path);
    Assert.assertEquals(record,value);
    Stat newstat = _zkClient.getStat(path);
    
    Assert.assertEquals(stat.getCtime(), newstat.getCtime());
    Assert.assertNotSame(stat.getMtime(), newstat.getMtime());
    Assert.assertTrue(stat.getMtime()< newstat.getMtime());
    
  }

  // START STANDARD STUFF TO START ZK SERVER
  private String _zkServerAddress;
  private List<ZkServer> _localZkServers;
  private ZkClient _zkClient;
  private ClusterDataAccessor _accessor;
  private String _clusterName;

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

  @BeforeTest(groups =
  { "unitTest" })
  public void setup() throws IOException, Exception
  {
    List<Integer> localPorts = new ArrayList<Integer>();
    localPorts.add(2300);
    localPorts.add(2301);

    _localZkServers = startLocalZookeeper(localPorts,
        System.getProperty("user.dir") + "/" + "zkdata", 2000);
    _zkServerAddress = "localhost:" + 2301;

    _zkClient = new ZkClient(_zkServerAddress);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _clusterName = "testCluster";
    _accessor = new ZKDataAccessor(_clusterName, _zkClient);
  }

  @AfterTest(groups =
  { "unitTest" })
  public void tearDown()
  {
    stopLocalZookeeper(_localZkServers);
  }

}

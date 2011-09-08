package com.linkedin.clustermanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IDefaultNameSpace;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStat;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.zk.ZKConnectionFactory;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;

// TODO: need to write multi-thread test cases
// TODO: need to write performance test for zk-property store
public class TestZKPropertyStore
{
  private static final Logger LOG = Logger.getLogger(TestZKPropertyStore.class);
  private List<ZkServer> _localZkServers;

  public class TestPropertyChangeListener 
  implements PropertyChangeListener<String>
  { 
    public boolean _propertyChangeReceived = false;

    @Override
    public void onPropertyChange(String key)
    {
      // TODO Auto-generated method stub
      LOG.info("property change, " + key);
      _propertyChangeReceived = true;
    }

  }
  
  public class TestUpdater implements DataUpdater<String>
  {

    @Override
    public String update(String currentData)
    {
      return "new " + currentData;
    }
    
  }
  
  @Test
  public void testInvocation() throws Exception
  {
    try 
    {
      String zkServers = "localhost:2188";
      String value = null;
      
      PropertyJsonSerializer<String> serializer = new PropertyJsonSerializer<String>(String.class);
      
      ZkConnection zkConn = ZKConnectionFactory.<String>create(zkServers, serializer);
      ZkConnection zkConnSame = ZKConnectionFactory.<String>create(zkServers, serializer);
      Assert.assertEquals(zkConn, zkConnSame);
 
      final String propertyStoreRoot = "/testZKPropertyStore";
      ZKPropertyStore<String> zkPropertyStore = new ZKPropertyStore<String>(zkConn, serializer, propertyStoreRoot);
      
      // test remove recursive and get non exist property
      zkPropertyStore.removeRootNamespace();
      value = zkPropertyStore.getProperty("nonExist");
      Assert.assertEquals(value, null);
  
      // test set/get property
      zkPropertyStore.setProperty("child1/grandchild1", "grandchild1");
      zkPropertyStore.setProperty("child1/grandchild2", "grandchild2");
        
      PropertyStat propertyStat = new PropertyStat();
      value = zkPropertyStore.getProperty("child1/grandchild1", propertyStat);
      Assert.assertEquals(value, "grandchild1");
      
      // test cache
      zkPropertyStore.setProperty("child1/grandchild1", "new grandchild1");
      value = zkPropertyStore.getProperty("child1/grandchild1", propertyStat);
      Assert.assertEquals(value, "new grandchild1");
      
      zkPropertyStore.setProperty("child1/grandchild1", "grandchild1");
      value = zkPropertyStore.getProperty("child1/grandchild1", propertyStat);
      Assert.assertEquals(value, "grandchild1");
      
      /**
      // test get property of a node without data
      value = zkPropertyStore.getProperty("");
      AssertJUnit.assertTrue(value == null);
  
      // test get property of a non-exist node
      value = zkPropertyStore.getProperty("abc");
      AssertJUnit.assertTrue(value == null);
      **/
      
      
      // test subscribe property
      TestPropertyChangeListener listener = new TestPropertyChangeListener();
      zkPropertyStore.subscribeForRootPropertyChange(listener);
      // Assert.assertEquals(listener._propertyChangeReceived, false);
  
      listener._propertyChangeReceived = false;
      zkPropertyStore.setProperty("child2/grandchild3", "grandchild3");
      Thread.sleep(100);
      Assert.assertEquals(listener._propertyChangeReceived, true);
      
      listener._propertyChangeReceived = false;
      zkPropertyStore.setProperty("child1/grandchild4", "grandchild4");
      Thread.sleep(100);
      Assert.assertEquals(listener._propertyChangeReceived, true);

      listener._propertyChangeReceived = false;
      zkPropertyStore.setProperty("child1/grandchild4", "new grandchild4");
      Thread.sleep(100);
      Assert.assertEquals(listener._propertyChangeReceived, true);

      // value = zkPropertyStore.getProperty("child2/grandchild3");
      // Assert.assertEquals(value, "grandchild3");
      
      // test remove an existing property
      // this triggers child change at both child1/grandchild4 and child1
      listener._propertyChangeReceived = false;
      zkPropertyStore.removeProperty("child1/grandchild4");  
      Thread.sleep(100);
      Assert.assertTrue(listener._propertyChangeReceived);
      
      /**
      // test remove property of a node with children
      boolean exceptionThrown = false;
      try
      {
        zkPropertyStore.removeProperty("");
      } catch (PropertyStoreException e)
      {
        // System.err.println(e.getMessage());
        exceptionThrown = true;
      }
      AssertJUnit.assertTrue(exceptionThrown);
      **/
      
      // test update property
      // boolean isSucceed;
      zkPropertyStore.updatePropertyUntilSucceed("child1/grandchild1", new TestUpdater());
      value = zkPropertyStore.getProperty("child1/grandchild1");
      Assert.assertEquals(value, "new grandchild1");
      
      // test compare and set
      boolean isSucceed = zkPropertyStore.compareAndSet("child1/grandchild1", 
                                                        "grandchild1", 
                                                        "new new grandchild1", 
                                                        new PropertyJsonComparator<String>(String.class));
      Assert.assertEquals(isSucceed, false);
      
      value = zkPropertyStore.getProperty("child1/grandchild1");
      AssertJUnit.assertTrue(value.equals("new grandchild1"));
      
      isSucceed = zkPropertyStore.compareAndSet("child1/grandchild1", 
                                                "new grandchild1", 
                                                "new new grandchild1", 
                                                new PropertyJsonComparator<String>(String.class));
      Assert.assertEquals(isSucceed, true);
      
      value = zkPropertyStore.getProperty("child1/grandchild1");
      AssertJUnit.assertTrue(value.equals("new new grandchild1"));
    
      // test compare and set, create if absent
      isSucceed = zkPropertyStore.compareAndSet("child2/grandchild5", 
                                                null, 
                                                "grandchild5",  
                                                new PropertyJsonComparator<String>(String.class),
                                                true);
      // Thread.sleep(100); // wait cache to be updated by callback
      Assert.assertEquals(isSucceed, true);
      
      value = zkPropertyStore.getProperty("/child2/grandchild5");
      Assert.assertEquals(value, "grandchild5");
      
      /**
      // test unsubscribe
      // wait for the previous callback to happen
      // then set _propertyChangeRecieved to false
      Thread.sleep(100);  
      listener._propertyChangeReceived = false;
      zkPropertyStore.unsubscribeForRootPropertyChange(listener);
      zkPropertyStore.setProperty("testPath3/2", "testData3_III");
      Thread.sleep(100);
      Assert.assertEquals(listener._propertyChangeReceived, false);
      **/
      
      // test get proper names
      List<String> children = zkPropertyStore.getPropertyNames("/child2");
      Assert.assertTrue(children != null);
      Assert.assertEquals(children.size(), 2);
      Assert.assertTrue(children.contains("/child2/grandchild3"));
      Assert.assertTrue(children.contains("/child2/grandchild5"));
      
      Thread.sleep(100);
    }
    catch(PropertyStoreException e)
    {
      e.printStackTrace();
    }
    
  }

  @BeforeTest
  public void setup() throws IOException
  {
    List<Integer> localPorts = new ArrayList<Integer>();
    localPorts.add(2188);
    // localPorts.add(2301);

    _localZkServers = startLocalZookeeper(localPorts,
        System.getProperty("user.dir") + "/" + "zkdata", 2000);

    System.out.println("zk servers started on ports: " + localPorts);
  }

  @AfterTest
  public void tearDown()
  {
    stopLocalZookeeper(_localZkServers);
    System.out.println("zk servers stopped");
  }
  
  // copy from TestZKCallback
  public static List<ZkServer> startLocalZookeeper(List<Integer> localPortsList, 
                                                   String zkTestDataRootDir, 
                                                   int tickTime)
    throws IOException
  {
    List<ZkServer> localZkServers = new ArrayList<ZkServer>();
  
    int count = 0;
    for (int port : localPortsList)
    {
      ZkServer zkServer = startZkServer(zkTestDataRootDir, count++, port, tickTime);
       localZkServers.add(zkServer);
     }
     return localZkServers;
   }
  
  public static ZkServer startZkServer(String zkTestDataRootDir, 
                                        int machineId,
                                        int port, 
                                        int tickTime) 
     throws IOException
  {
    File zkTestDataRootDirFile = new File(zkTestDataRootDir);
    zkTestDataRootDirFile.mkdirs();
  
    String dataPath = zkTestDataRootDir + "/" + machineId + "/" + port + "/data";
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
  
    ZkServer zkServer = new ZkServer(dataPath, logPath, mockDefaultNameSpace, port, tickTime);
    zkServer.start();
    
    return zkServer;
  }
   
  public static void stopLocalZookeeper(List<ZkServer> localZkServers)
  {
    for (ZkServer zkServer : localZkServers)
    {
      zkServer.shutdown();
    }
  }

}

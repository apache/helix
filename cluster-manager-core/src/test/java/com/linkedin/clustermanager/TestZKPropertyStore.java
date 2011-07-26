package com.linkedin.clustermanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import junit.framework.Assert;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertyStat;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.StringPropertySerializer;
import com.linkedin.clustermanager.store.zk.ZKConnectionFactory;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;

// TODO: need to write multi-thread test cases
// TODO: need to write performance test for zk-property store
public class TestZKPropertyStore
{
  private List<ZkServer> _localZkServers;

  public class MyPropertyChangeListener implements
      PropertyChangeListener<String>
  {
    public boolean _propertyChangeReceived = false;

    @Override
    public void onPropertyChange(String key)
    {
      // TODO Auto-generated method stub
      System.out.println("property changed at " + key);
      _propertyChangeReceived = true;
    }

  }
  
  public class MyUpdater implements DataUpdater<String>
  {

    @Override
    public String update(String currentData)
    {
      return currentData + "-new";
    }
    
  }
  
  public class MyComparator implements Comparator<String>
  {

    @Override
    public int compare(String o1, String o2)
    {
      if (o1 == null && o2 == null)
      {
        return 0;
      }
      else if (o1 == null && o2 != null)
      {
        return -1;
      }
      else if (o1 != null && o2 == null)
      {
        return 1;
      }
      else
      {
        return o1.compareTo(o2);
      }
    }
    
  }

  @Test
  public void testInvocation() throws Exception
  {
    try 
    {
      String zkServers = "localhost:2188";
      String value = null;
      
      StringPropertySerializer serializer = new StringPropertySerializer();
      
      ZkConnection zkConn = ZKConnectionFactory.<String>create(zkServers, serializer);
      ZkConnection zkConnSame = ZKConnectionFactory.<String>create(zkServers, serializer);
      Assert.assertEquals(zkConn, zkConnSame);
      
  
      final String propertyStoreRoot = "/testPath1";
      ZKPropertyStore<String> zkPropertyStore = new ZKPropertyStore<String>(zkConn, serializer, propertyStoreRoot);
      
      // test remove recursive and get non exist property
      zkPropertyStore.removeRootNamespace();
      value = zkPropertyStore.getProperty("nonExist");
      Assert.assertEquals(value, null);
  
      // test set/get property
      zkPropertyStore.setProperty("testPath2/1", "testData2_I");
      zkPropertyStore.setProperty("testPath2/2", "testData2_II");
        
      PropertyStat propertyStat = new PropertyStat();
      value = zkPropertyStore.getProperty("testPath2/1", propertyStat);
      
      
      Assert.assertEquals(value, "testData2_I");
  
      /**
      // test get property of a node without data
      value = zkPropertyStore.getProperty("");
      AssertJUnit.assertTrue(value == null);
  
      // test get property of a non-exist node
      value = zkPropertyStore.getProperty("abc");
      AssertJUnit.assertTrue(value == null);
      **/
      
      
      // test subscribe property
      MyPropertyChangeListener listener = new MyPropertyChangeListener();
      zkPropertyStore.subscribeForRootPropertyChange(listener);
      Assert.assertEquals(listener._propertyChangeReceived, false);
  
      zkPropertyStore.setProperty("testPath3/1", "testData3_I");
      Thread.sleep(100);
      Assert.assertEquals(listener._propertyChangeReceived, true);
     
      
      listener._propertyChangeReceived = false;
      zkPropertyStore.setProperty("testPath3/2", "testData3_II");
      Thread.sleep(100);
      Assert.assertEquals(listener._propertyChangeReceived, true);
  
      value = zkPropertyStore.getProperty("testPath3/1");
      Assert.assertEquals(value, "testData3_I");
      
      // test remove an existing property
      // this triggers child change at both /testPath1/testPath3/1 (weird) and /testPath1/testPath3 
      zkPropertyStore.removeProperty("testPath3/1");  
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
      boolean isSucceed;
      zkPropertyStore.updatePropertyUntilSucceed("testPath2/1", new MyUpdater());
      Thread.sleep(100); // wait cache to be updated by callback
      // Assert.assertTrue(isSucceed == true);
      
      value = zkPropertyStore.getProperty("testPath2/1");
      Assert.assertEquals(value, "testData2_I-new");
      
      // test compareAndSet property
      isSucceed = zkPropertyStore.compareAndSet("testPath2/1", "testData2_I", "testData2_I-new2", new MyComparator());
      Thread.sleep(100); // wait cache to be updated by callback
      Assert.assertEquals(isSucceed, false);
      
      value = zkPropertyStore.getProperty("testPath2/1");
      AssertJUnit.assertTrue(value.equals("testData2_I-new"));
      
      isSucceed = zkPropertyStore.compareAndSet("testPath2/1", "testData2_I-new", "testData2_I-new2", new MyComparator());
      Thread.sleep(100); // wait cache to be updated by callback
      Assert.assertEquals(isSucceed, true);
      
      value = zkPropertyStore.getProperty("testPath2/1");
      Assert.assertEquals(value, "testData2_I-new2");
    
      
      isSucceed = zkPropertyStore.compareAndSet("testPath2/3", null, "testData2_III", new MyComparator(), true);
      Thread.sleep(100); // wait cache to be updated by callback
      Assert.assertEquals(isSucceed, true);
      
      value = zkPropertyStore.getProperty("testPath2/3");
      Assert.assertEquals(value, "testData2_III");
      
      // test unsubscribe
      listener._propertyChangeReceived = false;
      zkPropertyStore.unsubscribeForRootPropertyChange(listener);
      zkPropertyStore.setProperty("testPath3/2", "testData3_III");
      Thread.sleep(100);
      Assert.assertEquals(listener._propertyChangeReceived, false);
  
      
      // test get proper names
      List<String> children = zkPropertyStore.getPropertyNames("/testPath2");
      Assert.assertTrue(children != null);
      Assert.assertEquals(children.size(), 3);
      Assert.assertEquals(children.get(0), "testPath2/3");
      Assert.assertEquals(children.get(1), "testPath2/2");
      
      Thread.sleep(100);
    }
    catch(PropertyStoreException e)
    {
      e.printStackTrace();
    }
    
    // test hit ratio
    /**
    value = zkPropertyStore.getProperty(testPath3);
    double hitRatio = zkPropertyStore.getHitRatio();
    AssertJUnit.assertTrue(Double.compare(Math.abs(hitRatio - 0.5), 0.1) < 0);
    **/
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
      public void createDefaultNameSpace(ZkClient zkClient)
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

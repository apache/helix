package com.linkedin.clustermanager;

import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStat;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;

// TODO need to write multi-thread test cases
// TODO need to write performance test for zk-property store
@Test (groups = {"unitTest"})
public class TestZKPropertyStore extends ZkUnitTestBase
{
  private static final Logger LOG = Logger.getLogger(TestZKPropertyStore.class);
  // private List<ZkServer> _localZkServers;

  private class TestPropertyChangeListener 
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
  
  private class TestUpdater implements DataUpdater<String>
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
    LOG.info("START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
    LOG.info("number of connections is " + ZkClient.getNumberOfConnections());

    try 
    {
      String value = null;
      
      PropertyJsonSerializer<String> serializer = new PropertyJsonSerializer<String>(String.class);
      
      ZkConnection zkConn = new ZkConnection(ZK_ADDR);

      final String propertyStoreRoot = "/" + getShortClassName();
      if (_zkClient.exists(propertyStoreRoot))
      {
        _zkClient.deleteRecursive(propertyStoreRoot);
      }

      ZKPropertyStore<String> zkPropertyStore = new ZKPropertyStore<String>(zkConn, serializer, propertyStoreRoot);
      
      // test remove recursive and get non exist property
      zkPropertyStore.removeRootNamespace();
      value = zkPropertyStore.getProperty("nonExist");
      AssertJUnit.assertEquals(value, null);
  
      // test set/get property
      zkPropertyStore.setProperty("child1/grandchild1", "grandchild1");
      zkPropertyStore.setProperty("child1/grandchild2", "grandchild2");
        
      PropertyStat propertyStat = new PropertyStat();
      value = zkPropertyStore.getProperty("child1/grandchild1", propertyStat);
      AssertJUnit.assertEquals(value, "grandchild1");
      
      // test cache
      zkPropertyStore.setProperty("child1/grandchild1", "new grandchild1");
      value = zkPropertyStore.getProperty("child1/grandchild1", propertyStat);
      AssertJUnit.assertEquals(value, "new grandchild1");
      
      zkPropertyStore.setProperty("child1/grandchild1", "grandchild1");
      value = zkPropertyStore.getProperty("child1/grandchild1", propertyStat);
      AssertJUnit.assertEquals(value, "grandchild1");
      
      /*
      // test get property of a node without data
      value = zkPropertyStore.getProperty("");
      AssertJUnit.assertTrue(value == null);
  
      // test get property of a non-exist node
      value = zkPropertyStore.getProperty("abc");
      AssertJUnit.assertTrue(value == null);
      */
      
      // test subscribe property
      TestPropertyChangeListener listener = new TestPropertyChangeListener();
      zkPropertyStore.subscribeForRootPropertyChange(listener);
      // Assert.assertEquals(listener._propertyChangeReceived, false);
  
      listener._propertyChangeReceived = false;
      zkPropertyStore.setProperty("child2/grandchild3", "grandchild3");
      Thread.sleep(100);
      AssertJUnit.assertEquals(listener._propertyChangeReceived, true);
      
      listener._propertyChangeReceived = false;
      zkPropertyStore.setProperty("child1/grandchild4", "grandchild4");
      Thread.sleep(100);
      AssertJUnit.assertEquals(listener._propertyChangeReceived, true);

      listener._propertyChangeReceived = false;
      zkPropertyStore.setProperty("child1/grandchild4", "new grandchild4");
      Thread.sleep(100);
      AssertJUnit.assertEquals(listener._propertyChangeReceived, true);

      // value = zkPropertyStore.getProperty("child2/grandchild3");
      // Assert.assertEquals(value, "grandchild3");
      
      // test remove an existing property
      // this triggers child change at both child1/grandchild4 and child1
      listener._propertyChangeReceived = false;
      zkPropertyStore.removeProperty("child1/grandchild4");  
      Thread.sleep(100);
      AssertJUnit.assertTrue(listener._propertyChangeReceived);
      
      /*
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
      */
      
      // test update property
      // boolean isSucceed;
      zkPropertyStore.updatePropertyUntilSucceed("child1/grandchild1", new TestUpdater());
      value = zkPropertyStore.getProperty("child1/grandchild1");
      AssertJUnit.assertEquals(value, "new grandchild1");
      
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
      AssertJUnit.assertEquals(value, "grandchild5");
      
      // test unsubscribe
      // wait for the previous callback to happen
      // then set _propertyChangeRecieved to false
      Thread.sleep(100);  
      listener._propertyChangeReceived = false;
      zkPropertyStore.unsubscribeForRootPropertyChange(listener);
      zkPropertyStore.setProperty("/child1/grandchild1", "new new new grandchild1");
      Thread.sleep(100);
      AssertJUnit.assertEquals(listener._propertyChangeReceived, false);
      
      // test get proper names
      List<String> children = zkPropertyStore.getPropertyNames("/child2");
      AssertJUnit.assertTrue(children != null);
      AssertJUnit.assertEquals(children.size(), 2);
      AssertJUnit.assertTrue(children.contains("/child2/grandchild3"));
      AssertJUnit.assertTrue(children.contains("/child2/grandchild5"));
      
      Thread.sleep(100);
    }
    catch(PropertyStoreException e)
    {
      e.printStackTrace();
    }
    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
    LOG.info("number of connections is " + ZkClient.getNumberOfConnections());
  }
}

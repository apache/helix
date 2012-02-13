package com.linkedin.helix.store.zk;

import java.util.Date;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyChangeListener;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;
import com.linkedin.helix.store.PropertyStat;
import com.linkedin.helix.store.PropertyStoreException;
import com.linkedin.helix.store.zk.ZKPropertyStore;

// TODO need to write multi-thread test cases
// TODO need to write performance test for zk-property store
public class TestZKPropertyStore extends ZkUnitTestBase
{
  private static final Logger LOG = Logger.getLogger(TestZKPropertyStore.class);

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

	ZkClient _zkClient;

	@BeforeClass
	public void beforeClass()
	{
		_zkClient = new ZkClient(ZK_ADDR);
		_zkClient.setZkSerializer(new ZNRecordSerializer());
	}

	@AfterClass
	public void afterClass()
	{
		_zkClient.close();
	}


  @Test ()
  public void testInvocation() throws Exception
  {
  	System.out.println("START TestZKPropertyStore at" + new Date(System.currentTimeMillis()));
    LOG.info("number of connections is " + ZkClient.getNumberOfConnections());

    try
    {
      String value = null;

      PropertyJsonSerializer<String> serializer = new PropertyJsonSerializer<String>(String.class);

//      ZkConnection zkConn = new ZkConnection(ZK_ADDR);

      final String propertyStoreRoot = "/" + getShortClassName();
      if (_zkClient.exists(propertyStoreRoot))
      {
        _zkClient.deleteRecursive(propertyStoreRoot);
      }

      ZKPropertyStore<String> zkPropertyStore 
        = new ZKPropertyStore<String>(new ZkClient(ZK_ADDR), serializer, propertyStoreRoot);

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


      // test get property of a non-exist node
      value = zkPropertyStore.getProperty("nonExist");
      AssertJUnit.assertNull(value);

      zkPropertyStore.createPropertyNamespace("child3");
      AssertJUnit.assertTrue(zkPropertyStore.exists("child3"));
      _zkClient.createPersistent(propertyStoreRoot + "/child3/grandchild31", "grandchild31");
      value = zkPropertyStore.getProperty("child3/grandchild31");
      AssertJUnit.assertEquals("grandchild31", value);
      _zkClient.writeData(propertyStoreRoot + "/child3/grandchild31", "new grandchild31");
      Thread.sleep(1000);
      value = zkPropertyStore.getProperty("child3/grandchild31");
      AssertJUnit.assertEquals("new grandchild31", value);
      _zkClient.delete(propertyStoreRoot + "/child3/grandchild31");
      Thread.sleep(1000);
      value = zkPropertyStore.getProperty("child3/grandchild31");
      AssertJUnit.assertNull(value);

      String root = zkPropertyStore.getPropertyRootNamespace();
      AssertJUnit.assertEquals(propertyStoreRoot, root);

      List<String> childs = zkPropertyStore.getPropertyNames("child4");
      AssertJUnit.assertEquals(0, childs.size());

      boolean exceptionCaught = false;
      try
      {
        zkPropertyStore.setPropertyDelimiter("/");
      } catch (PropertyStoreException e)
      {
        exceptionCaught = true;
      }
      AssertJUnit.assertTrue(exceptionCaught);
      AssertJUnit.assertFalse(zkPropertyStore.canParentStoreData());


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

      // test remove an existing property
      // this triggers child change at both child1/grandchild4 and child1
      listener._propertyChangeReceived = false;
      zkPropertyStore.removeProperty("child1/grandchild4");
      Thread.sleep(100);
      AssertJUnit.assertTrue(listener._propertyChangeReceived);


//      // test remove property of a node with children
//      boolean exceptionThrown = false;
//      try
//      {
//        zkPropertyStore.removeProperty("");
//      } catch (PropertyStoreException e)
//      {
//        // System.err.println(e.getMessage());
//        exceptionThrown = true;
//      }
//      AssertJUnit.assertTrue(exceptionThrown);

      // test update property
      zkPropertyStore.updatePropertyUntilSucceed("child1/grandchild1", new TestUpdater());
      value = zkPropertyStore.getProperty("child1/grandchild1");
      AssertJUnit.assertEquals(value, "new grandchild1");

      // test compare and set
      boolean isSucceed = zkPropertyStore.compareAndSet("child1/grandchild1",
                                                        "grandchild1",
                                                        "new new grandchild1",
                                                        new PropertyJsonComparator<String>(String.class));
      AssertJUnit.assertEquals(isSucceed, false);

      value = zkPropertyStore.getProperty("child1/grandchild1");
      AssertJUnit.assertTrue(value.equals("new grandchild1"));

      isSucceed = zkPropertyStore.compareAndSet("child1/grandchild1",
                                                "new grandchild1",
                                                "new new grandchild1",
                                                new PropertyJsonComparator<String>(String.class));
      AssertJUnit.assertEquals(isSucceed, true);

      value = zkPropertyStore.getProperty("child1/grandchild1");
      AssertJUnit.assertTrue(value.equals("new new grandchild1"));

      // test compare and set, create if absent
      isSucceed = zkPropertyStore.compareAndSet("child2/grandchild5",
                                                null,
                                                "grandchild5",
                                                new PropertyJsonComparator<String>(String.class),
                                                true);
      AssertJUnit.assertEquals(isSucceed, true);

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

    System.out.println("END TestZKPropertyStore at" + new Date(System.currentTimeMillis()));
    LOG.info("number of connections is " + ZkClient.getNumberOfConnections());
  }
}

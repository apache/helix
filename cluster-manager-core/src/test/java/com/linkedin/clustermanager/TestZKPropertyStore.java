package com.linkedin.clustermanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertySerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.zk.ZKClientFactory;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;

public class TestZKPropertyStore
{
  private List<ZkServer> _localZkServers;

  public class MyStringSerializer implements PropertySerializer
  {

    @Override
    public byte[] serialize(Object data) throws PropertyStoreException
    {
      String value = (String) data;

      return value.getBytes();
    }

    @Override
    public Object deserialize(byte[] bytes) throws PropertyStoreException
    {

      return new String(bytes);
    }

  }

  public class MyPropertyChangeListener implements PropertyChangeListener<String>
  {
    public boolean _propertyChangeReceived = false;

    @Override
    public void onPropertyChange(String key)
    {
      // TODO Auto-generated method stub
      // System.out.println("property changed at " + key);
      _propertyChangeReceived = true;
    }

  }

  @Test
  public void testInvocation() throws Exception
  {
    String zkServers = "localhost:2188";
    ZKClientFactory zkClientFactory = new ZKClientFactory();
    ZkClient zkClient = zkClientFactory.create(zkServers);

    String propertyStoreRoot = "/testPath1";
    ZKPropertyStore<String> zkPropertyStore = new ZKPropertyStore<String>(zkClient,
        new MyStringSerializer(), propertyStoreRoot);

    // test remove recursive
    zkPropertyStore.removeRootProperty();

    // test set property
    String testPath2 = "testPath2";
    String testData2 = "testData2_I";
    zkPropertyStore.setProperty(testPath2, testData2);

    String value = zkPropertyStore.getProperty(testPath2);
    AssertJUnit.assertTrue(value.equals(testData2));

    // test get property of a node without data
    value = zkPropertyStore.getProperty("");
    AssertJUnit.assertTrue(value == null);

    // test get property of a non-exist node
    value = zkPropertyStore.getProperty("abc");
    AssertJUnit.assertTrue(value == null);

    // test subscribe property
    MyPropertyChangeListener listener = new MyPropertyChangeListener();
    zkPropertyStore.subscribeForRootPropertyChange(listener);
    AssertJUnit.assertTrue(!listener._propertyChangeReceived);

    String testPath3 = "testPath3";
    String testData3 = "testData3_I";
    zkPropertyStore.setProperty(testPath3, testData3);
    Thread.sleep(100);
    AssertJUnit.assertTrue(listener._propertyChangeReceived);

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

    // test subscribe
    listener._propertyChangeReceived = false;
    zkPropertyStore.removeProperty(testPath2);
    Thread.sleep(100);
    AssertJUnit.assertTrue(listener._propertyChangeReceived);

    listener._propertyChangeReceived = false;
    zkPropertyStore.setProperty(testPath3, "testData3_II");
    Thread.sleep(100);
    AssertJUnit.assertTrue(listener._propertyChangeReceived);

    // test unsubscribe
    listener._propertyChangeReceived = false;
    zkPropertyStore.unsubscribeForRootPropertyChange(listener);
    zkPropertyStore.setProperty(testPath3, "testData3_III");
    Thread.sleep(100);
    AssertJUnit.assertTrue(!listener._propertyChangeReceived);

    // test get proper names
    List<String> children = zkPropertyStore.getPropertyNames("");
    AssertJUnit.assertTrue(children != null && children.size() == 1
        && children.get(0).equals(testPath3));

  }

  @BeforeTest
  public void setup() throws IOException
  {
    List<Integer> localPorts = new ArrayList<Integer>();
    localPorts.add(2188);
    // localPorts.add(2301);

    _localZkServers = TestZKCallback.startLocalZookeeper(localPorts, System.getProperty("user.dir")
        + "/" + "zkdata", 2000);

    System.out.println("zk servers started on ports: " + localPorts);
  }

  @AfterTest
  public void tearDown()
  {
    TestZKCallback.stopLocalZookeeper(_localZkServers);
    System.out.println("zk servers stopped");
  }
}

package com.linkedin.clustermanager;

import java.util.Date;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;

@Test (groups = {"unitTest"})
public class TestZkPropertyStoreSessionExpiry extends ZkUnitTestBase
{
  private static final Logger LOG = Logger.getLogger(TestZkPropertyStoreSessionExpiry.class);

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

  @Test
  public void testZkPropertyStoreSessionExpiry() throws Exception
  {
    LOG.info("START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

    PropertyJsonSerializer<String> serializer = new PropertyJsonSerializer<String>(String.class);
    
    ZkConnection zkConn = new ZkConnection(ZK_ADDR);

    final String propertyStoreRoot = "/" + getShortClassName();
    if (_zkClient.exists(propertyStoreRoot))
    {
      _zkClient.deleteRecursive(propertyStoreRoot);
    }

    ZKPropertyStore<String> zkPropertyStore = new ZKPropertyStore<String>(zkConn, serializer, propertyStoreRoot);
    
    zkPropertyStore.setProperty("/child1/grandchild1", "grandchild1");
    zkPropertyStore.setProperty("/child1/grandchild2", "grandchild2");
    
    TestPropertyChangeListener listener = new TestPropertyChangeListener();
    zkPropertyStore.subscribeForRootPropertyChange(listener);

    listener._propertyChangeReceived = false;
    zkPropertyStore.setProperty("/child2/grandchild3", "grandchild3");
    Thread.sleep(100);
    AssertJUnit.assertEquals(listener._propertyChangeReceived, true);

    simulateSessionExpiry(zkConn);
    
    listener._propertyChangeReceived = false;
    zkPropertyStore.setProperty("/child2/grandchild4", "grandchild4");
    Thread.sleep(100);
    AssertJUnit.assertEquals(listener._propertyChangeReceived, true);

    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

  }
}

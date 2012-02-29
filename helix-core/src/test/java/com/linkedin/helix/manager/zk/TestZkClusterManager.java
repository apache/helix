package com.linkedin.helix.manager.zk;

import java.util.Date;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.MockListener;
import com.linkedin.helix.store.PropertyStore;

public class TestZkClusterManager extends ZkUnitTestBase
{
  @Test()
  public void testZkClusterManager()
  {
    System.out.println("START TestZkClusterManager.beforeClass() at " + new Date(System.currentTimeMillis()));

    final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
    try
    {
      if (_gZkClient.exists("/" + clusterName))
      {
        _gZkClient.deleteRecursive("/" + clusterName);
      }

      ZKHelixManager controller = new ZKHelixManager(clusterName, null,
                                                         InstanceType.CONTROLLER,
                                                         ZK_ADDR);

      try
      {
        controller.connect();
        Assert.fail("Should throw HelixException if initial cluster structure is not setup");
      } catch (HelixException e)
      {
        // OK
      }


      TestHelper.setupEmptyCluster(_gZkClient, clusterName);

      AssertJUnit.assertEquals(-1, controller.getLastNotificationTime());
      controller.connect();
      AssertJUnit.assertTrue(controller.isConnected());
      controller.connect();
      AssertJUnit.assertTrue(controller.isConnected());

      MockListener listener = new MockListener();
      listener.reset();

      try
      {
        controller.addControllerListener(null);
        Assert.fail("Should throw HelixException");
      } catch (HelixException e)
      {
        // OK
      }

      controller.addControllerListener(listener);
      AssertJUnit.assertTrue(listener.isControllerChangeListenerInvoked);
      controller.removeListener(listener);

      PropertyStore<ZNRecord> store = controller.getPropertyStore();
      ZNRecord record = new ZNRecord("id1");
      store.setProperty("key1", record);
      record = store.getProperty("key1");
      AssertJUnit.assertEquals("id1", record.getId());

      controller.getMessagingService();
      controller.getHealthReportCollector();
      controller.getClusterManagmentTool();

      controller.handleNewSession();
      controller.disconnect();
      AssertJUnit.assertFalse(controller.isConnected());
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    System.out.println("END TestZkClusterManager.beforeClass() at " + new Date(System.currentTimeMillis()));
  }

}

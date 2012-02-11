package com.linkedin.helix.agent.zk;

import java.util.Date;

import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.agent.MockListener;
import com.linkedin.helix.agent.zk.ZKHelixAgent;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.store.PropertyStore;

public class TestZkClusterManager extends ZkUnitTestBase
{
	ZkClient _zkClient;

	@BeforeClass
	public void beforeClass()
	{
		System.out.println("START TestZkClusterManager.beforeClass() at " + new Date(System.currentTimeMillis()));
	  _zkClient = new ZkClient(ZK_ADDR);
		_zkClient.setZkSerializer(new ZNRecordSerializer());
  }

	@AfterClass
	public void afterClass()
	{
		_zkClient.close();
		System.out.println("END TestZkClusterManager.beforeClass() at " + new Date(System.currentTimeMillis()));
	}

  @Test()
  public void testZkClusterManager()
  {
    final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
    try
    {
      if (_zkClient.exists("/" + clusterName))
      {
        _zkClient.deleteRecursive("/" + clusterName);
      }

      TestHelper.setupEmptyCluster(_zkClient, clusterName);
      ZKHelixAgent controller = new ZKHelixAgent(clusterName, null,
                                                         InstanceType.CONTROLLER,
                                                         ZK_ADDR);

      AssertJUnit.assertEquals(-1, controller.getLastNotificationTime());
      controller.connect();
      AssertJUnit.assertTrue(controller.isConnected());
      controller.connect();
      AssertJUnit.assertTrue(controller.isConnected());

      MockListener listener = new MockListener();
      listener.reset();

      boolean exceptionCaught = false;
      try
      {
        controller.addControllerListener(null);
      } catch (HelixException e)
      {
        exceptionCaught = true;
      }
      AssertJUnit.assertTrue(exceptionCaught);

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
  }

}

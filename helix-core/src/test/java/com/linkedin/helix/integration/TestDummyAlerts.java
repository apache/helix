package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterDataAccessor;
import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.agent.zk.ZKDataAccessor;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.controller.ClusterManagerMain;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockTransitionIntf;
import com.linkedin.helix.model.Message;

public class TestDummyAlerts extends ZkIntegrationTestBase
{
  ZkClient _zkClient;

  @BeforeClass()
  public void beforeClass() throws Exception
  {
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
  }

  @AfterClass
  public void afterClass()
  {
    _zkClient.close();
  }

  public class DummyAlertsTransition implements MockTransitionIntf
  {
    @Override
    public void doTrasition(Message message, NotificationContext context)
    {
      ClusterManager manager = context.getManager();
      ClusterDataAccessor accessor = manager.getDataAccessor();
      String fromState = message.getFromState();
      String toState = message.getToState();
      String instance = message.getTgtName();
      String partition = message.getStateUnitKey();

      if (fromState.equalsIgnoreCase("SLAVE") && toState.equalsIgnoreCase("MASTER"))
      {
        for (int i = 0; i < 5; i++)
        {
          accessor.setProperty(PropertyType.HEALTHREPORT, new ZNRecord("mockAlerts" + i), instance,
              "mockAlerts");
          try
          {
            Thread.sleep(1000);
          } catch (InterruptedException e)
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
    }
  }

  @Test()
  public void testDummyAlerts() throws Exception
  {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START TestDummyAlerts at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant start
                                                         // port
        "localhost", // participant name prefix
        "TestDB", // resource group name prefix
        1, // resource groups
        10, // partitions per resource group
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    TestHelper.startController(clusterName, "controller_0", ZK_ADDR, ClusterManagerMain.STANDALONE);
    // start participants
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR,
          new DummyAlertsTransition());
      new Thread(participants[i]).start();
    }

    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewExtended",
        30 * 1000, // timeout in millisecond
        ZK_ADDR, TestHelper.<String> setOf(clusterName), TestHelper.<String> setOf("TestDB0"),
        null, null, null);

    // other verifications go here
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    for (int i = 0; i < 5; i++)
    {
      String instance = "localhost_" + (12918 + i);
      ZNRecord record = accessor.getProperty(PropertyType.HEALTHREPORT, instance, "mockAlerts");
      Assert.assertEquals(record.getId(), "mockAlerts4");
    }

    System.out.println("END TestDummyAlerts at " + new Date(System.currentTimeMillis()));
  }
}

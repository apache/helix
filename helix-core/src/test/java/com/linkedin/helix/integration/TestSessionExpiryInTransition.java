package com.linkedin.helix.integration;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;

import com.linkedin.helix.InstanceType;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZkTestHelper;
import com.linkedin.helix.ZkTestHelper.TestZkHelixManager;
import com.linkedin.helix.mock.controller.StandaloneController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockTransition;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;

public class TestSessionExpiryInTransition extends ZkIntegrationTestBase
{

  public class SessionExpiryTransition extends MockTransition
  {
    private final AtomicBoolean _done = new AtomicBoolean();

    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      TestZkHelixManager manager = (TestZkHelixManager)context.getManager();
     
      String instance = message.getTgtName();
      String partition = message.getPartitionName();
      if (instance.equals("localhost_12918")
          && partition.equals("TestDB0_1")  // TestDB0_1 is SLAVE on localhost_12918
          && _done.getAndSet(true) == false)
      {
        try
        {
          ZkTestHelper.expireSession(manager.getZkClient());
        }
        catch (Exception e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }
 
  // TODO: disable test first until we have a clean design in handling zk disconnect/session-expiry
  // when there is pending messages
  // @Test
  public void testSessionExpiryInTransition() throws Exception
  {
    Logger.getRootLogger().setLevel(Level.WARN);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[5];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // start controller
    StandaloneController controller =
        new StandaloneController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
      TestZkHelixManager manager =
          new TestZkHelixManager(clusterName,
                                 instanceName,
                                 InstanceType.PARTICIPANT,
                                 ZK_ADDR);
      participants[i] = new MockParticipant(manager, new SessionExpiryTransition());
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // clean up
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    Thread.sleep(2000);
    controller.syncStop();

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }
}
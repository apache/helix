package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.mock.controller.StandaloneController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockTransition;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;

public class TestEnablePartitionDuringDisable extends ZkIntegrationTestBase
{
  static
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
  }

  class EnablePartitionTransition extends MockTransition
  {
    int slaveToOfflineCnt = 0;
    int offlineToSlave = 0;

    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      HelixManager manager = context.getManager();
      String clusterName = manager.getClusterName();

      String instance = message.getTgtName();
      String partitionName = message.getPartitionName();
      String fromState = message.getFromState();
      String toState = message.getToState();
      if (instance.equals("localhost_12919") && partitionName.equals("TestDB0_0"))
      {
        if (fromState.equals("SLAVE") && toState.equals("OFFLINE"))
        {
          slaveToOfflineCnt++;

          try
          {
            String command =
                "--zkSvr " + ZK_ADDR + " --enablePartition true " + clusterName
                    + " localhost_12919 TestDB0 TestDB0_0";

            ClusterSetup.processCommandLineArgs(command.split("\\s+"));
          }
          catch (Exception e)
          {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

        }
        else if (slaveToOfflineCnt > 0 && fromState.equals("OFFLINE") && toState.equals("SLAVE"))
        {
          offlineToSlave++;
        }
      }
    }

  }

  @Test
  public void testEnablePartitionDuringDisable() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            10, // partitions per resource
                            5, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    StandaloneController controller =
        new StandaloneController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();

    // start participants
    EnablePartitionTransition transition = new EnablePartitionTransition();
    MockParticipant[] participants = new MockParticipant[5];
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      if (instanceName.equals("localhost_12919"))
      {
        participants[i] =
            new MockParticipant(clusterName,
                                instanceName,
                                ZK_ADDR,
                                transition);
      }
      else
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      }
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // disable partitions
    String command =
        "--zkSvr " + ZK_ADDR + " --enablePartition false " + clusterName
            + " localhost_12919 TestDB0 TestDB0_0";

    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // ensure we get 1 slaveToOffline and 1 offlineToSlave after disable partition
    long startT = System.currentTimeMillis();
    while (System.currentTimeMillis() - startT < 1000)  // retry in 1s
    {
      if (transition.slaveToOfflineCnt > 0 && transition.offlineToSlave > 0)
      {
        break;
      }

      Thread.sleep(10);
    }
    long endT = System.currentTimeMillis();
    System.out.println("1 disable and re-enable took: " + (endT - startT) + "ms");
    Assert.assertEquals(transition.slaveToOfflineCnt, 1, "should get 1 slaveToOffline transition");
    Assert.assertEquals(transition.offlineToSlave, 1, "should get 1 offlineToSlave transition");

    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }
}

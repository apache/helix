package com.linkedin.helix.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockParticipant.ErrTransition;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;

public class TestResetPartitionState extends ZkIntegrationTestBase
{
  int _errToOfflineInvoked = 0;

  class ErrTransitionWithReset extends ErrTransition
  {
    public ErrTransitionWithReset(Map<String, Set<String>> errPartitions)
    {
      super(errPartitions);
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
    {
      // System.err.println("doRest() invoked");
      super.doTransition(message, context);
      String fromState = message.getFromState();
      String toState = message.getToState();
      if (fromState.equals("ERROR") && toState.equals("OFFLINE"))
      {
        _errToOfflineInvoked++;
      }
    }

  }

  @Test()
  public void testResetPartitionState() throws Exception
  {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
                            1, 10, 5, 3, "MasterSlave", true);

    TestHelper.startController(clusterName, "controller_0",
                                      ZK_ADDR, HelixControllerMain.STANDALONE);
    Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>()
    {
      {
        put("SLAVE-MASTER", TestHelper.setOf("TestDB0_0"));
        put("OFFLINE-SLAVE", TestHelper.setOf("TestDB0_8"));
      }
    };
    for (int i = 0; i < 5; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (12918 + i);

      if (i == 0)
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR,
                                new ErrTransition(errPartitions));
      }
      else
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR);
      }
      new Thread(participants[i]).start();
    }

    Map<String, Set<String>> errorStateMap = new HashMap<String, Set<String>>()
    {
      {
        put("TestDB0_0", TestHelper.setOf("localhost_12918"));
        put("TestDB0_8", TestHelper.setOf("localhost_12918"));
      }
    };

    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewExtended",
                                 ZK_ADDR,
                                 TestHelper.<String>setOf(clusterName),
                                 TestHelper.<String>setOf("TestDB0"),
                                 null,
                                 null,
                                 errorStateMap);

    // reset one error partition
    errPartitions.remove("SLAVE-MASTER");
    participants[0].setTransition(new ErrTransitionWithReset(errPartitions));
    clearStatusUpdate(clusterName, "localhost_12918", "TestDB0", "TestDB0_0");
    _errToOfflineInvoked = 0;
    ZKHelixAdmin tool = new ZKHelixAdmin(_gZkClient);
    tool.resetPartition(clusterName, "localhost_12918", "TestDB0", "TestDB0_0");

    errorStateMap.remove("TestDB0_0");
    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewExtended",
                                 ZK_ADDR,
                                 TestHelper.<String>setOf(clusterName),
                                 TestHelper.<String> setOf("TestDB0"),
                                 null,
                                 null,
                                 errorStateMap);
    Assert.assertEquals(_errToOfflineInvoked, 1);

    // reset the other error partition
    participants[0].setTransition(new ErrTransitionWithReset(null));
    clearStatusUpdate(clusterName, "localhost_12918", "TestDB0", "TestDB0_8");
    tool.resetPartition(clusterName, "localhost_12918", "TestDB0", "TestDB0_8");

    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewExtended",
                                 ZK_ADDR,
                                 TestHelper.<String> setOf(clusterName),
                                 TestHelper.<String> setOf("TestDB0"),
                                 null,
                                 null,
                                 null);
    Assert.assertEquals(_errToOfflineInvoked, 2);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  private void clearStatusUpdate(String clusterName, String instance, String resource,
      String partition)
  {
    // clear status update for error partition so verify() will not fail on old
    // errors
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _gZkClient);
    LiveInstance liveInstance = accessor.getProperty(LiveInstance.class,
        PropertyType.LIVEINSTANCES, instance);
    accessor.removeProperty(PropertyType.STATUSUPDATES, instance,
        liveInstance.getSessionId(),
        resource, partition);

   }
  // TODO: throw exception in reset()
}

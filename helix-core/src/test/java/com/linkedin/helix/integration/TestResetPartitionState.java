package com.linkedin.helix.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.agent.zk.ZKClusterManagementTool;
import com.linkedin.helix.agent.zk.ZKDataAccessor;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.controller.ClusterManagerMain;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.mock.storage.MockParticipant.ErrTransition;
import com.linkedin.helix.model.LiveInstance;

// TODO need to rewrite reset logic and this test
public class TestResetPartitionState extends ZkIntegrationTestBase
{
  ZkClient _zkClient;
  
  @BeforeClass ()
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

  @Test()
  public void testResetPartitionState() throws Exception
  {
    String clusterName = getShortClassName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
                            1, 10, 5, 3, "MasterSlave", true);

    TestHelper.startController(clusterName, "controller_0",
                                      ZK_ADDR, ClusterManagerMain.STANDALONE);
    for (int i = 0; i < 5; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (12918 + i);

      if (i == 0)
      {
        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR,
            new ErrTransition("SLAVE", "MASTER", TestHelper.setOf("TestDB0_0")));
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
      }
    };

    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewExtended",
                                 ZK_ADDR,
                                 TestHelper.<String>setOf(clusterName),
                                 TestHelper.<String>setOf("TestDB0"),
                                 null,
                                 null,
                                 errorStateMap);

    // reset state model
    participants[0].resetTransition();

    // clear status update for error partition to avoid confusing the verifier
    ZKDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);
    LiveInstance localhost12918 = accessor.getProperty(LiveInstance.class, 
                                                       PropertyType.LIVEINSTANCES,
                                                       "localhost_12918");
    accessor.removeProperty(PropertyType.STATUSUPDATES, 
                            "localhost_12918", 
                            localhost12918.getSessionId(),
                            "TestDB0",
                            "TestDB0_0");
    
    ZKClusterManagementTool tool = new ZKClusterManagementTool(_zkClient);
    tool.resetPartition(clusterName, "localhost_12918", "TestDB0", "TestDB0_0");

    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 ZK_ADDR,
                                 TestHelper.<String>setOf(clusterName),
                                 TestHelper.<String>setOf("TestDB0"));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}

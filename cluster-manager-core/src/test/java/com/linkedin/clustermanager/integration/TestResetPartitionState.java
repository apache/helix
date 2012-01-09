package com.linkedin.clustermanager.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.agent.zk.ZKClusterManagementTool;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.mock.storage.ErroneousDummyParticipant;

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
    ErroneousDummyParticipant[] participants = new ErroneousDummyParticipant[5];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB",
                            1, 10, 5, 3, "MasterSlave", true);

    TestHelper.startClusterController(clusterName, "controller_0",
                                      ZK_ADDR, ClusterManagerMain.STANDALONE, null);
    for (int i = 0; i < 5; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (12918 + i);

      participants[i] = new ErroneousDummyParticipant(clusterName, instanceName, ZK_ADDR);
      if (i == 0)
      {
        participants[0].setIsErroneous("TestDB0_0", "SLAVE", "MASTER", true);
      }
      new Thread(participants[i]).start();
    }


    Map<String, String> errorStateMap = new HashMap<String, String>()
    {
      {
        put("TestDB0_0", "localhost_12918");
      }
    };

    TestHelper.verifyWithTimeout("verifyBestPossAndExtViewExtended",
                                 "TestDB0",
                                 10,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(clusterName),
                                 _zkClient,
                                 null,
                                 null,
                                 errorStateMap);


    // correct state model and reset partition
    participants[0].setIsErroneous("TestDB0_0", "SLAVE", "MASTER", false);

    ZKClusterManagementTool tool = new ZKClusterManagementTool(_zkClient);
    tool.resetPartition(clusterName, "localhost_12918", "TestDB0", "TestDB0_0");

    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "TestDB0",
                                 10,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(clusterName),
                                 _zkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}

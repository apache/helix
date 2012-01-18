package com.linkedin.clustermanager.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.StartCMResult;
import com.linkedin.clustermanager.agent.zk.ZKClusterManager;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterSetup;

public class TestStandAloneCMSessionExpiry extends ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(TestStandAloneCMSessionExpiry.class);
  protected final String CLUSTER_NAME = "CLUSTER_" + "TestStandAloneCMSessionExpiry";
  protected static final int NODE_NR = 5;
  protected Map<String, StartCMResult> _startCMResultMap = new HashMap<String, StartCMResult>();

  class ZkClusterManagerWithSessionExpiry extends ZKClusterManager
  {
    public ZkClusterManagerWithSessionExpiry(String clusterName, String instanceName,
                                             InstanceType instanceType,
                                             String zkConnectString) throws Exception
    {
      super(clusterName, instanceName, instanceType, zkConnectString);
      // TODO Auto-generated constructor stub
    }

    public void expireSession() throws Exception
    {
      ZkIntegrationTestBase.simulateSessionExpiry(_zkClient);
    }
  }

  @Test()
  public void testStandAloneCMSessionExpiry()
    throws Exception
  {
    System.out.println("RUN testStandAloneCMSessionExpiry() at " + new Date(System.currentTimeMillis()));


    ZkClient zkClient = new ZkClient(ZK_ADDR);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);

    TestHelper.setupCluster(CLUSTER_NAME,
                            ZK_ADDR,
                            12918,
                            PARTICIPANT_PREFIX,
                            "TestDB",
                            1,
                            20,
                            NODE_NR,
                            3,
                            "MasterSlave",
                            true);
    // start dummy participants
    Map<String, ZkClusterManagerWithSessionExpiry> managers = new HashMap<String, ZkClusterManagerWithSessionExpiry>();
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
      ZkClusterManagerWithSessionExpiry manager = new ZkClusterManagerWithSessionExpiry(CLUSTER_NAME,
                                                                                        instanceName,
                                                                                        InstanceType.PARTICIPANT,
                                                                                        ZK_ADDR);
      managers.put(instanceName, manager);
      Thread thread = new Thread(new TestHelper.DummyProcessThread(manager, instanceName));
      thread.start();
    }

    // start controller
    String controllerName = "controller_0";

    ZkClusterManagerWithSessionExpiry manager = new ZkClusterManagerWithSessionExpiry(CLUSTER_NAME,
                                                                                      controllerName,
                                                                                      InstanceType.CONTROLLER,
                                                                                      ZK_ADDR);
    manager.connect();
    managers.put(controllerName, manager);

    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "TestDB0",
                                 20,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 zkClient);

    managers.get("localhost_12918").expireSession();

    setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB", 10, "MasterSlave");
    setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 3);

    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "TestDB0",
                                 20,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 zkClient);

    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "MyDB",
                                 10,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 zkClient);

    managers.get(controllerName).expireSession();

    setupTool.addResourceGroupToCluster(CLUSTER_NAME, "MyDB2", 8, "MasterSlave");
    setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB2", 3);

    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "TestDB0",
                                 20,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 zkClient);


    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "MyDB",
                                 10,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 zkClient);

    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 "MyDB2",
                                 8,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 zkClient);

    System.out.println("STOP testStandAloneCMSessionExpiry() at " + new Date(System.currentTimeMillis()));
  }

}

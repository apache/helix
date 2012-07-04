package com.linkedin.helix.integration;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.model.StateModelDefinition.StateModelDefinitionProperty;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;

public class TestNonOfflineInitState extends ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(TestNonOfflineInitState.class);

  @Test
  public void testNonOfflineInitState() throws Exception
  {
    System.out.println("START testNonOfflineInitState at "
        + new Date(System.currentTimeMillis()));
    String clusterName = getShortClassName();


    setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                 "localhost", // participant name prefix
                 "TestDB", // resource name prefix
                 1, // resources
                 10, // partitions per resource
                 5, // number of nodes
                 1, // replicas
                 "Bootstrap",
                 true); // do rebalance

    TestHelper.startController(clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);
    
    // start participants
    MockParticipant[] participants = new MockParticipant[5];
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] =
          new MockParticipant(clusterName,
                              instanceName,
                              ZK_ADDR,
                              null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    
    System.out.println("END testNonOfflineInitState at "
        + new Date(System.currentTimeMillis()));
  }

  private static void setupCluster(String clusterName,
                                   String ZkAddr,
                                   int startPort,
                                   String participantNamePrefix,
                                   String resourceNamePrefix,
                                   int resourceNb,
                                   int partitionNb,
                                   int nodesNb,
                                   int replica,
                                   String stateModelDef,
                                   boolean doRebalance) throws Exception
  {
    ZkClient zkClient = new ZkClient(ZkAddr);
    if (zkClient.exists("/" + clusterName))
    {
      LOG.warn("Cluster already exists:" + clusterName + ". Deleting it");
      zkClient.deleteRecursive("/" + clusterName);
    }

    ClusterSetup setupTool = new ClusterSetup(ZkAddr);
    setupTool.addCluster(clusterName, true);
    setupTool.addStateModelDef(clusterName,
                               "Bootstrap",
                               new StateModelDefinition(generateConfigForBootstrap()));

    for (int i = 0; i < nodesNb; i++)
    {
      int port = startPort + i;
      setupTool.addInstanceToCluster(clusterName, participantNamePrefix + ":" + port);
    }

    for (int i = 0; i < resourceNb; i++)
    {
      String dbName = resourceNamePrefix + i;
      setupTool.addResourceToCluster(clusterName, dbName, partitionNb, stateModelDef);
      if (doRebalance)
      {
        setupTool.rebalanceStorageCluster(clusterName, dbName, replica);
      }
    }
    zkClient.close();
  }

  private static ZNRecord generateConfigForBootstrap()
  {
    ZNRecord record = new ZNRecord("Bootstrap");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "IDLE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("ONLINE");
    statePriorityList.add("BOOTSTRAP");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("IDLE");
    statePriorityList.add("DROPPED");
    statePriorityList.add("ERROR");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
                        statePriorityList);
    for (String state : statePriorityList)
    {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("ONLINE"))
      {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      }
      else if (state.equals("BOOTSTRAP"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("OFFLINE"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("IDLE"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("DROPPED"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      else if (state.equals("ERROR"))
      {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }

    for (String state : statePriorityList)
    {
      String key = state + ".next";
      if (state.equals("ONLINE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("BOOTSTRAP", "OFFLINE");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        metadata.put("IDLE", "OFFLINE");
        record.setMapField(key, metadata);
      }
      else if (state.equals("BOOTSTRAP"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONLINE", "ONLINE");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        metadata.put("IDLE", "OFFLINE");
        record.setMapField(key, metadata);
      }
      else if (state.equals("OFFLINE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONLINE", "BOOTSTRAP");
        metadata.put("BOOTSTRAP", "BOOTSTRAP");
        metadata.put("DROPPED", "IDLE");
        metadata.put("IDLE", "IDLE");
        record.setMapField(key, metadata);
      }
      else if (state.equals("IDLE"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONLINE", "OFFLINE");
        metadata.put("BOOTSTRAP", "OFFLINE");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      }
      else if (state.equals("ERROR"))
      {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("IDLE", "IDLE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("ONLINE-OFFLINE");
    stateTransitionPriorityList.add("BOOTSTRAP-ONLINE");
    stateTransitionPriorityList.add("OFFLINE-BOOTSTRAP");
    stateTransitionPriorityList.add("BOOTSTRAP-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-IDLE");
    stateTransitionPriorityList.add("IDLE-OFFLINE");
    stateTransitionPriorityList.add("IDLE-DROPPED");
    stateTransitionPriorityList.add("ERROR-IDLED");
    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
                        stateTransitionPriorityList);
    return record;
  }
}

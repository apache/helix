package com.linkedin.helix.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.integration.ZkStandAloneCMTestBase;
import com.linkedin.helix.josql.JsqlQueryListProcessor;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateProperty;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;

public class TestControllerRebalancingTimer extends ZkStandAloneCMTestBase
{
  private static Logger                LOG               =
      Logger.getLogger(TestControllerRebalancingTimer.class);

  @Override
  @BeforeClass
  public void beforeClass() throws Exception
  {
//    Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START " + CLASS_NAME + " at "
        + new Date(System.currentTimeMillis()));

    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    String namespace = "/" + CLUSTER_NAME;
    if (_zkClient.exists(namespace))
    {
      _zkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      String storageNodeName = PARTICIPANT_PREFIX + ":" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);
    IdealState idealState = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    idealState.getRecord().setSimpleField(IdealStateProperty.REBALANCE_TIMER_PERIOD.toString(), "500");

    String scnTableQuery = "SELECT T1.instance as instance, T1.mapField as partition, T1.gen as gen, T1.seq as seq " +
        "FROM explodeMap(`INSTANCES/*/HEALTHREPORT/scnTable`) AS T1" +
        " JOIN LIVEINSTANCES as T2 using (T1.instance, T2.id)";

    String rankQuery = "SELECT instance, partition, gen, seq, T1.listIndex AS instanceRank " +
            " FROM scnTable JOIN explodeList(`IDEALSTATES/" + TEST_DB + "`) AS T1 " +
                    "USING (scnTable.instance, T1.listVal) WHERE scnTable.partition=T1.listField";

    String masterSelectionQuery = "SELECT instance, partition, instanceRank, gen, (T.maxSeq-seq) AS seqDiff, seq FROM rankTable JOIN " +
            " (SELECT partition, max(to_number(seq)) AS maxSeq FROM rankTable GROUP BY partition) AS T USING(rankTable.partition, T.partition) " +
            " WHERE to_number(seqDiff) < 10 " +
            " ORDER BY partition, to_number(gen) desc, to_number(instanceRank), to_number(seqDiff)";

    StringBuffer combinedQueryStringList = new StringBuffer();
    combinedQueryStringList.append(scnTableQuery + JsqlQueryListProcessor.SEPARATOR+"scnTable;");
    combinedQueryStringList.append(rankQuery + JsqlQueryListProcessor.SEPARATOR+"rankTable;");
    combinedQueryStringList.append(masterSelectionQuery);

    String command = "-zkSvr " + ZK_ADDR + " -addResourceProperty "+ CLUSTER_NAME + " " + TEST_DB + " " + IdealState.QUERY_LIST.toString() + " "
        ;//+ "\""+ combinedQueryStringList.toString() +"\"";
    String[] args = command.split(" ");

    List<String> argsList = new ArrayList<String>();
    argsList.addAll(Arrays.asList(args));
    argsList.add("\""+ combinedQueryStringList.toString() +"\"");
    String[] allArgs = new String[argsList.size()];
    argsList.toArray(allArgs);
    ClusterSetup.processCommandLineArgs(allArgs);

    command = "-zkSvr " + ZK_ADDR + " -addResourceProperty "+ CLUSTER_NAME + " " + TEST_DB + " " + IdealState.IdealStateProperty.REBALANCE_TIMER_PERIOD.toString() + " 500";

    ClusterSetup.processCommandLineArgs(command.split(" "));


    // start dummy participants
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      if (_startCMResultMap.get(instanceName) != null)
      {
        LOG.error("fail to start particpant:" + instanceName
            + "(participant with same name already exists)");
      }
      else
      {
        StartCMResult result =
            TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName);
        _startCMResultMap.put(instanceName, result);
      }
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    StartCMResult startResult =
        TestHelper.startController(CLUSTER_NAME,
                                   controllerName,
                                   ZK_ADDR,
                                   HelixControllerMain.STANDALONE);
    _startCMResultMap.put(controllerName, startResult);

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              CLUSTER_NAME));

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @Test
  public void TestRebalancingTimer() throws Exception
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    GenericHelixController controller = new GenericHelixController();
    _startCMResultMap.get(controllerName)._manager.addIdealStateChangeListener(controller);

    Assert.assertTrue(controller._rebalanceTimer != null);
    Assert.assertEquals(controller._timerPeriod, 500);

    String command = "-zkSvr " + ZK_ADDR + " -addResourceProperty "+ CLUSTER_NAME + " " + TEST_DB + " " + IdealState.IdealStateProperty.REBALANCE_TIMER_PERIOD.toString() + " 200";

    ClusterSetup.processCommandLineArgs(command.split(" "));

    Thread.sleep(1000);
    Assert.assertTrue(controller._rebalanceTimer != null);
    Assert.assertEquals(controller._timerPeriod, 200);
  }

  @Test
  public void testMasterSelectionBySCN() throws InterruptedException
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    StartCMResult startResult =
    _startCMResultMap.get(controllerName);
    HelixDataAccessor accessor = startResult._manager.getHelixDataAccessor();

    IdealState idealState = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    Map<String, ZNRecord> scnTableMap = new HashMap<String, ZNRecord>();
    for (int i = 0; i < NODE_NR; i++)
    {
      String instance = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      ZNRecord scnRecord = new ZNRecord("scnTable");
      scnRecord.setSimpleField("instance", instance);
      scnTableMap.put(instance, scnRecord);
    }
    for(int j = 0; j < _PARTITIONS; j++)
    {
      int seq = 50;
      String partition = TEST_DB + "_" + j;
      List<String> idealStatePrefList =
          idealState.getPreferenceList(partition);
      String idealStateMaster = idealStatePrefList.get(0);


      for(int x = 0; x < idealStatePrefList.size(); x++)
      {
        String instance = idealStatePrefList.get(x);
        ZNRecord scnRecord = scnTableMap.get(instance);
        if(!scnRecord.getMapFields().containsKey(partition))
        {
          scnRecord.setMapField(partition, new HashMap<String, String>());
        }
        Map<String, String> scnDetails = scnRecord.getMapField(partition);
        scnDetails.put("gen", "4");
        if(x > 0)
        {
          scnDetails.put("seq", "" + (seq - 22 + 10 *(x)));
        }
        else
        {
          scnDetails.put("seq", "" + (seq));
        }
      }
    }

    for(String instanceName : scnTableMap.keySet())
    {
      Builder kb = accessor.keyBuilder();
      accessor.setProperty(kb.healthReport(instanceName, "scnTable"), new HealthStat(scnTableMap.get(instanceName)));
    }

    String instanceDead = PARTICIPANT_PREFIX + "_" + (START_PORT + 0);
    _startCMResultMap.get(instanceDead)._manager.disconnect();
    _startCMResultMap.get(instanceDead)._thread.interrupt();

    Thread.sleep(3000);
    Builder kb = accessor.keyBuilder();
    ExternalView ev = accessor.getProperty(kb.externalView(TEST_DB));
    for(String partitionName : idealState.getPartitionSet())
    {
      List<String> prefList = idealState.getPreferenceList(partitionName);
      if(prefList.get(0).equals(instanceDead))
      {
        String last = prefList.get(prefList.size() - 1);
        Assert.assertTrue(ev.getStateMap(partitionName).get(last).equals("MASTER"));
      }
    }
    // kill a node, after a while the master should be the last one in the ideal state pref list
  }
}

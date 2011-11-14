package com.linkedin.clustermanager.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.StartCMResult;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.tools.ClusterSetup;

/**
 *
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants verify the current states at end
 */

public class ZkStandAloneCMTestBase extends ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(ZkStandAloneCMTestBase.class);

  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";
  protected static final String TEST_DB = "TestDB";

  protected ZkClient _controllerZkClient;
  protected ZkClient[] _participantZkClients = new ZkClient[NODE_NR];
  protected ClusterSetup _setupTool = null;
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;

  protected Map<String, StartCMResult> _startCMResultMap = new HashMap<String, StartCMResult>();
  protected ZkClient _zkClient;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    // logger.info("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

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
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, TEST_DB, 20, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      String storageNodeName = PARTICIPANT_PREFIX + ":" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);

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
        _participantZkClients[i] = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
        StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR,
                                                            CLUSTER_NAME,
                                                            instanceName,
                                                            _participantZkClients[i]);
        _startCMResultMap.put(instanceName, result);
      }
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controllerZkClient = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
    StartCMResult startResult = TestHelper.startClusterController(CLUSTER_NAME,
                                                                  controllerName,
                                                                  ZK_ADDR,
                                                                  ClusterManagerMain.STANDALONE,
                                                                  _controllerZkClient);
    _startCMResultMap.put(controllerName, startResult);

    verifyCluster();
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    /**
     * shutdown order:
     *   1) disconnect the controller
     *   2) disconnect participants
     */

    StartCMResult result;
    Iterator<Entry<String, StartCMResult>> it = _startCMResultMap.entrySet().iterator();
    while (it.hasNext())
    {
      String instanceName = it.next().getKey();
      if (instanceName.startsWith(CONTROLLER_PREFIX))
      {
        result = _startCMResultMap.get(instanceName);
        result._manager.disconnect();
        result._thread.interrupt();
        it.remove();
      }
    }

    it = _startCMResultMap.entrySet().iterator();
    while (it.hasNext())
    {
      String instanceName = it.next().getKey();
      result = _startCMResultMap.get(instanceName);
      result._manager.disconnect();
      result._thread.interrupt();
      it.remove();
    }

    _zkClient.close();

    // logger.info("END at " + new Date(System.currentTimeMillis()));
    System.out.println("END " + CLASS_NAME + " at "+ new Date(System.currentTimeMillis()));
  }

  protected void verifyCluster()
  {
    TestHelper.verifyWithTimeout("verifyBestPossAndExtView",
                                 TEST_DB,
                                 20,
                                 "MasterSlave",
                                 TestHelper.<String>setOf(CLUSTER_NAME),
                                 _zkClient);
  }
}

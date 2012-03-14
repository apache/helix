package com.linkedin.helix.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.PauseSignal;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;

/**
 * setup 10 storage clusters and a special controller cluster
 * start 5 cluster controllers in distributed mode
 * start 5 dummy participants in the first storage cluster
 * at end, verify the current states of both the controller cluster
 * and the first storage cluster
 *
 * @author zzhang
 *
 */

public class ZkDistCMTestBase extends ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(ZkDistCMTestBase.class);

  protected static final int CLUSTER_NR = 10;
  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";
  protected ClusterSetup _setupTool = null;
  protected Map<String, StartCMResult> _startCMResultMap = new HashMap<String, StartCMResult>();

  protected final String CLASS_NAME = getShortClassName();
  protected final String CONTROLLER_CLUSTER = CONTROLLER_CLUSTER_PREFIX + "_" + CLASS_NAME;

  protected static final String TEST_DB = "TestDB";
  ZkClient _zkClient;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    System.out.println("START " + CLASS_NAME + ".b4Class() at " + new Date(System.currentTimeMillis()));
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());

    String namespace = "/" + CONTROLLER_CLUSTER;
    if (_zkClient.exists(namespace))
    {
      _zkClient.deleteRecursive(namespace);
    }

    for (int i = 0; i < CLUSTER_NR; i++)
    {
      namespace = "/" + CLUSTER_PREFIX + "_" + CLASS_NAME + "_" + i;
      if (_zkClient.exists(namespace))
      {
        _zkClient.deleteRecursive(namespace);
      }
    }

    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup cluster of clusters
    for (int i = 0; i < CLUSTER_NR; i++)
    {
      String clusterName = CLUSTER_PREFIX + "_" + CLASS_NAME + "_" + i;
      _setupTool.addCluster(clusterName, true);
    }

    final String firstCluster = CLUSTER_PREFIX + "_" + CLASS_NAME + "_0";
    setupStorageCluster(_setupTool, firstCluster, TEST_DB, 20, PARTICIPANT_PREFIX,
                        START_PORT, "MasterSlave", 3);

    // setup CONTROLLER_CLUSTER
    _setupTool.addCluster(CONTROLLER_CLUSTER, true);
    setupStorageCluster(_setupTool, CONTROLLER_CLUSTER,
                        CLUSTER_PREFIX + "_" + CLASS_NAME, CLUSTER_NR,
                        CONTROLLER_PREFIX, 0, "LeaderStandby", 3);

    // start dummy participants for the first cluster
    for (int i = 0; i < 5; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      if (_startCMResultMap.get(instanceName) != null)
      {
        LOG.error("fail to start participant:" + instanceName
                     + "(participant with the same name already running");
      }
      else
      {
        StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, firstCluster,
                                                            instanceName);
        _startCMResultMap.put(instanceName, result);
      }
    }

    // start distributed cluster controllers
    for (int i = 0; i < 5; i++)
    {
      String controllerName = CONTROLLER_PREFIX + "_" + i;
      if (_startCMResultMap.get(controllerName) != null)
      {
        LOG.error("fail to start controller:" + controllerName
                     + "(controller with the same name already running");
      }
      else
      {
        StartCMResult result = TestHelper.startController(CONTROLLER_CLUSTER,
                                                                 controllerName,
                                                                 ZK_ADDR,
                                                                 HelixControllerMain.DISTRIBUTED);
        _startCMResultMap.put(controllerName, result);
      }
    }

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewVerifier(ZK_ADDR, CONTROLLER_CLUSTER));
    Assert.assertTrue(result);
    
    result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewVerifier(ZK_ADDR, CLUSTER_PREFIX + "_" + CLASS_NAME + "_0"));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    System.out.println("AFTERCLASS " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    /**
     * shutdown order:
     *   1) pause the leader (optional)
     *   2) disconnect all controllers
     *   3) disconnect leader/disconnect participant
     */
    String leader = getCurrentLeader(_zkClient, CONTROLLER_CLUSTER);
    // pauseController(_startCMResultMap.get(leader)._manager.getDataAccessor());

    StartCMResult result;

    Iterator<Entry<String, StartCMResult>> it = _startCMResultMap.entrySet().iterator();

    while (it.hasNext())
    {
      String instanceName = it.next().getKey();
      if (!instanceName.equals(leader) && instanceName.startsWith(CONTROLLER_PREFIX))
      {
        result = _startCMResultMap.get(instanceName);
        result._manager.disconnect();
        result._thread.interrupt();
        it.remove();
      }
      
      boolean verifyResult = ClusterStateVerifier.verify(
          new ClusterStateVerifier.BestPossAndExtViewVerifier(ZK_ADDR, CONTROLLER_CLUSTER));
      Assert.assertTrue(verifyResult);
      
      verifyResult = ClusterStateVerifier.verify(
          new ClusterStateVerifier.BestPossAndExtViewVerifier(ZK_ADDR, CLUSTER_PREFIX + "_" + CLASS_NAME + "_0"));
      Assert.assertTrue(verifyResult);

    }

    result = _startCMResultMap.remove(leader);
    result._manager.disconnect();
    result._thread.interrupt();

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

    // logger.info("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  protected void pauseController(DataAccessor clusterDataAccessor)
  {
    clusterDataAccessor.setProperty(PropertyType.PAUSE, new PauseSignal("pause"));
  }

  protected void setupStorageCluster(ClusterSetup setupTool, String clusterName,
       String dbName, int partitionNr, String prefix, int startPort, String stateModel, int replica)
  {
    setupTool.addResourceToCluster(clusterName, dbName, partitionNr, stateModel);
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = prefix + ":" + (startPort + i);
      setupTool.addInstanceToCluster(clusterName, instanceName);
    }
    setupTool.rebalanceStorageCluster(clusterName, dbName, replica);
  }
}

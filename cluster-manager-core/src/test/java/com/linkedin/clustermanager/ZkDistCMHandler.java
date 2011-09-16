package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.util.CMUtil;

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
public class ZkDistCMHandler
{
  private static Logger logger = Logger.getLogger(ZkDistCMHandler.class);
  protected static final String ZK_ADDR = "localhost:2181";
  protected static final String CLUSTER_PREFIX = "ESPRESSO_STORAGE";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  protected static final int CLUSTER_NR = 10;
  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";
  protected ClusterSetup _setupTool = null;
  protected Map<String, Thread> _threadMap = new HashMap<String, Thread>();
  
  private static final String TEST_DB = "TestDB";
  private ZkServer _zkServer = null;

  @BeforeClass
  public void beforeClass()
  {
    logger.info("START ZkDistCMHandler at " + new Date(System.currentTimeMillis()));
    List<String> namespaces = new ArrayList<String>();
    
    final String controllerCluster = CONTROLLER_CLUSTER_PREFIX + "_" + this.getClass().getName(); 
    namespaces.add("/" + controllerCluster);
    for (int i = 0; i < CLUSTER_NR; i++)
    {
      String clusterName = CLUSTER_PREFIX + "_" + this.getClass().getName() + "_" + i;
      namespaces.add("/" + clusterName);
    }
    
    _zkServer = TestHelper.startZkSever(ZK_ADDR, namespaces);
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup cluster of ESPRESSO_STORAGE clusters
    for (int i = 0; i < CLUSTER_NR; i++)
    {
      String clusterName = CLUSTER_PREFIX + "_" + this.getClass().getName() + "_" + i;
      _setupTool.addCluster(clusterName, true);
    }
    
    final String firstCluster = CLUSTER_PREFIX + "_" + this.getClass().getName() + "_0";
    setupStorageCluster(_setupTool, firstCluster, TEST_DB, 20, START_PORT, STATE_MODEL);
    
    // setup CONTROLLER_CLUSTER
    _setupTool.addCluster(controllerCluster, true);
    setupStorageCluster(_setupTool, controllerCluster, 
          CLUSTER_PREFIX + "_" + this.getClass().getName(), CLUSTER_NR, 8900, "LeaderStandby");
    
    // start dummy participants for the first ESPRESSO_STORAGE cluster
    Thread thread;
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (START_PORT + i);
      if (_threadMap.get(instanceName) != null)
      {
        logger.error("fail to start participant:" + instanceName + 
          " because there is already a thread with same instanceName running");
      }
      else
      {
        thread = TestHelper.startDummyProcess(ZK_ADDR, firstCluster, instanceName, null);
        _threadMap.put(instanceName, thread);
      }
    }

    // start distributed cluster controllers
    for (int i = 0; i < NODE_NR; i++)
    {
      String controllerName = "localhost_" + (8900 + i);
      if (_threadMap.get(controllerName) != null)
      {
        logger.error("fail to start controller:" + controllerName + 
          " because there is already a thread with same controllerName running");
      }
      else
      {
        thread = TestHelper.startClusterController(controllerCluster, controllerName, ZK_ADDR, 
           ClusterManagerMain.DISTRIBUTED, null);
        _threadMap.put(controllerName, thread);
      }
    }

    try
    {
      Thread.sleep(10000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    List<String> clusterNames = new ArrayList<String>();
    clusterNames.add(controllerCluster);
    clusterNames.add(firstCluster);
    verifyIdealAndCurrentState(clusterNames);
  }
  
  @AfterClass
  public void afterClass() throws Exception
  {
    logger.info("END ZkDistCMHandler at " + new Date(System.currentTimeMillis()));
    for (Map.Entry<String, Thread> entry : _threadMap.entrySet())
    {
      entry.getValue().interrupt();
    }
    TestHelper.stopZkServer(_zkServer);
  }
  
  protected void setupStorageCluster(ClusterSetup setupTool, String clusterName, 
       String dbName, int partitionNr, int startPort, String stateModel)
  {
    setupTool.addResourceGroupToCluster(clusterName, dbName, partitionNr, stateModel);
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost:" + (startPort + i);
      setupTool.addInstanceToCluster(clusterName, instanceName);
    }
    setupTool.rebalanceStorageCluster(clusterName, dbName, 3);
  }
  
  protected void verifyIdealAndCurrentState(List<String> clusterNames)
  {
    for (String clusterName : clusterNames)
    {
      boolean result = ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, clusterName);
      logger.info("verify cluster: " + clusterName + ", result: " + result);
      Assert.assertTrue(result);
    }
  }

  protected void stopCurrentLeader(String clusterName)
  {
    String leaderPath = CMUtil
        .getControllerPropertyPath(clusterName, ControllerPropertyType.LEADER);
    final ZkClient zkClient = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
    ZNRecord leaderRecord = zkClient.<ZNRecord>readData(leaderPath);
    Assert.assertTrue(leaderRecord != null);
    String controller = leaderRecord.getSimpleField(ControllerPropertyType.LEADER.toString());
    logger.info("stop current leader:" + controller);
    Assert.assertTrue(controller != null);
    Thread thread = _threadMap.remove(controller);
    Assert.assertTrue(thread != null);
    thread.interrupt();
  }
  
  protected void assertLeader(String clusterName)
  {
    final ZkClient zkClient = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
    String leaderPath = CMUtil
        .getControllerPropertyPath(clusterName, ControllerPropertyType.LEADER);
    ZNRecord leaderRecord = zkClient.<ZNRecord>readData(leaderPath);
    Assert.assertTrue(leaderRecord != null);
    String controller = leaderRecord.getSimpleField(ControllerPropertyType.LEADER.toString());
    logger.info("new leader is selected for controller cluster:" + controller);
    Assert.assertTrue(controller != null);
    Thread thread = _threadMap.get(controller);
    Assert.assertTrue(thread != null);
  }
}

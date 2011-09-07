package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.tools.StateModelConfigGenerator;
import com.linkedin.clustermanager.util.CMUtil;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestDistClusterController
{
  private static Logger LOG = Logger.getLogger(TestDistClusterController.class);
  private static final String _zkAddr = "localhost:2181";
  private static ZkServer _zkServer = null;
  private static ZkClient _zkClient;
  
  private static final String _cntrlClusterName = "CONTROLLER_CLUSTER";
  private static final String _storageClusterGroupName = "ESPRESSO_STORAGE";
  private static final int _storageClusterNr = 10;
  private static final String _distContrlStateModelName = "LeaderStandby";
  private static final String _espressoStorageModelName = "MasterSlave";
  private static final int _distClusterControllerNr = 5;
  private static final int _replica = 3;

  @Test
  public void testInvocation() throws Exception
  {
    LOG.info("Run at " + new Date(System.currentTimeMillis()));
    // start zk server
    List<String> namespaces = new ArrayList<String>();
    namespaces.add("/" + _cntrlClusterName);
    for (int i = 0; i < _storageClusterNr; i++)
    {
      String storageClusterNamespace = "/" + _storageClusterGroupName + "_" + i;
      namespaces.add(storageClusterNamespace);
    }
    
    _zkServer = TestHelper.startZkSever(_zkAddr, namespaces);
    _zkClient = ZKClientPool.getZkClient(_zkAddr);
    ClusterSetup setupTool = new ClusterSetup(_zkAddr);
    
    for (int i = 0; i < _storageClusterNr; i++)
    {
      String clusterName = _storageClusterGroupName + "_" + i;
      setupTool.addCluster(clusterName, true);
    }
    
    // setup storage clusters, ESPRESSO_STORAGE_0 ...
    setupStorageCluster(setupTool, "ESPRESSO_STORAGE_0", "TestDB", 12918);

    // setup CONTROLLER_CLUSTER
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    setupTool.addCluster(_cntrlClusterName, false, "LeaderStandby", generator.generateConfigForLeaderStandby());
    setupTool.addResourceGroupToCluster(_cntrlClusterName, _storageClusterGroupName, 
                                        _storageClusterNr, _distContrlStateModelName);
    
    for (int i = 0; i < _distClusterControllerNr; i++)
    {
      setupTool.addInstanceToCluster(_cntrlClusterName, "localhost", 8900 + i);
    }
    
    setupTool.rebalanceStorageCluster(_cntrlClusterName, _storageClusterGroupName, _replica);
    
    
    // start dummy storage node for ESPRESSO_STORAGE_0
    for (int i = 0; i < 5; i++)
    {
      TestHelper.startDummyProcess(_zkAddr, "ESPRESSO_STORAGE_0", "localhost_" + (12918 + i));
    }
    
    // start distributed cluster controllers
    Map<String, Thread> distControllerMap = new HashMap<String, Thread>();
    for (int i = 0; i < _distClusterControllerNr; i++)
    {
      String instanceName = "localhost_" + (8900 + i);
      Thread thread = TestHelper.startDistClusterController(_cntrlClusterName, 
                                                            instanceName, _zkAddr);
      distControllerMap.put(instanceName, thread);
    }

    Thread.sleep(10000);
    
    // verify ideal state == current state
    // String idealStatePath = CMUtil.getIdealStatePath(_cntrlClusterName, ClusterPropertyType.IDEALSTATES);
    boolean result = ClusterStateVerifier.VerifyClusterStates(_zkAddr, _cntrlClusterName);
    LOG.info("verify cluster: " + _cntrlClusterName + ", result: " + result);
    Assert.assertEquals(true, result);
    List<String> clusterNames = new ArrayList<String>();
    clusterNames.add("ESPRESSO_STORAGE_0");
    verifyIdealAndCurrentState(clusterNames);
    
    
    // stop current distributed cluster controller
    String leaderPath = CMUtil.getControllerPropertyPath(_cntrlClusterName, 
                                                         ControllerPropertyType.LEADER);
    ZNRecord leaderRecord = _zkClient.<ZNRecord>readData(leaderPath);
    Thread thread = distControllerMap.get(leaderRecord.getSimpleField("Leader"));
    thread.interrupt();
    
    Thread.sleep(3000);
    result = _zkClient.exists(leaderPath);
    LOG.info("new distributed clsuter controller exists: " + result);
    Assert.assertEquals(true, result);
    
    // setup storage cluster: ESPRESSO_STORAGE_1 ...
    setupStorageCluster(setupTool, "ESPRESSO_STORAGE_1", "MyDB", 13918);
    
    // start dummy storage node for ESPRESSO_STORAGE_1
    for (int i = 0; i < 5; i++)
    {
      TestHelper.startDummyProcess(_zkAddr, "ESPRESSO_STORAGE_1", "localhost_" + (13918 + i));
    }

    Thread.sleep(10000);
    clusterNames.add("ESPRESSO_STORAGE_0");
    clusterNames.add("ESPRESSO_STORAGE_1");
    verifyIdealAndCurrentState(clusterNames);

    LOG.info("End at " + new Date(System.currentTimeMillis()));
    
    // stop zk server
    TestHelper.stopZkServer(_zkServer);
  }
  
  private void setupStorageCluster(ClusterSetup setupTool, String clusterName, String dbName,
                                   int startPort)
  {
    setupTool.addResourceGroupToCluster(clusterName, dbName, 20, _espressoStorageModelName);
    for (int  j = 0; j < 5; j++)
    {
      String storageNodeName = "localhost:" + (startPort + j);
      setupTool.addInstanceToCluster(clusterName, storageNodeName);
    }
    setupTool.rebalanceStorageCluster(clusterName, dbName, 3);
  }
  
  private void verifyIdealAndCurrentState(List<String> clusterNames)
  {
    for (String clusterName : clusterNames)
    {
      boolean result = ClusterStateVerifier.VerifyClusterStates(_zkAddr, clusterName);
      LOG.info("verify cluster: " + clusterName + ", result: " + result);
      Assert.assertEquals(true, result);
    }
  }
}

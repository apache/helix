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

import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;

public class TestDistClusterController
{
  private static Logger LOG = Logger.getLogger(TestDistClusterController.class);
  private static final String _zkAddr = "localhost:2181";
  private static ZkServer _zkServer = null;
  
  private static final String _cntrlClusterName = "CONTROLLER_CLUSTER";
  private static final String _storageClusterGroupName = "ESPRESSO_STORAGE";
  private static final int _storageClusterNr = 10;
  private static final String stateModelName = "MasterSlave";
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

    // setup CONTROLLER_CLUSTER
    ClusterSetup setupTool = new ClusterSetup(_zkAddr);
    setupTool.addCluster(_cntrlClusterName, false);
    setupTool.addResourceGroupToCluster(_cntrlClusterName, _storageClusterGroupName, 
                                        _storageClusterNr, stateModelName);
    
    for (int i = 0; i < _distClusterControllerNr; i++)
    {
      setupTool.addNodeToCluster(_cntrlClusterName, "localhost",8900 + i);
    }
    
    setupTool.rebalanceStorageCluster(_cntrlClusterName, _storageClusterGroupName, 
                                      _replica);
    
    // start distributed cluster controller
    Map<String, Thread> distControllerMap = new HashMap<String, Thread>();
    for (int i = 0; i < _distClusterControllerNr; i++)
    {
      String instanceName = "localhost_" + (8900 + i);
      Thread thread = TestHelper.startDistClusterController(_cntrlClusterName, 
                                                            instanceName, _zkAddr);
      distControllerMap.put(instanceName, thread);
    }

    Thread.sleep(10000);
    
    boolean result = ClusterStateVerifier.VerifyClusterStates(_zkAddr, _cntrlClusterName);
    LOG.info("verify cluster: " + _cntrlClusterName + ", result: " + result);
    Assert.assertEquals(true, result);
    
    LOG.info("End at " + new Date(System.currentTimeMillis()));
    // stop zk server
    TestHelper.stopZkServer(_zkServer);
  }
}

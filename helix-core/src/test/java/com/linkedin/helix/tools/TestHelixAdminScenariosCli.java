package com.linkedin.helix.tools;
/*
 * Simulate all the admin tasks needed by using command line tool
 * 
 * */
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.integration.ZkIntegrationTestBase;
import com.linkedin.helix.manager.zk.ZKUtil;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.LiveInstance;

public class TestHelixAdminScenariosCli extends ZkIntegrationTestBase
{
  
  void assertException(String command)
  {
    boolean exceptionThrown = false;
    try
    {
      ClusterSetup.processCommandLineArgs(command.split(" "));
    }
    catch(Exception e)
    {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }
  
  @Test
  public void TestAddDeleteClusterAndInstanceAndResource() throws Exception
  {
    Map<String, StartCMResult> _startCMResultMap =
        new HashMap<String, StartCMResult>();
    
    ZkClient zkClient = new ZkClient("localhost:2183");
    
    /**======================= Add clusters ==============================*/
    
    String command = "--zkSvr localhost:2183 -addCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // malformed cluster name 
    command = "--zkSvr localhost:2183 -addCluster /ClusterTest";
    assertException(command);
    
    command = "--zkSvr localhost:2183 -addCluster \"Klazt3rz";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "--zkSvr localhost:2183 -addCluster \\ClusterTest";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // Add already exist cluster
    command = "--zkSvr localhost:2183 -addCluster clusterTest";
    assertException(command);
    
    // delete cluster without resource and instance
    Assert.assertTrue(ZKUtil.isClusterSetup("Klazt3rz", zkClient));
    Assert.assertTrue(ZKUtil.isClusterSetup("clusterTest", zkClient));
    Assert.assertTrue(ZKUtil.isClusterSetup("\\ClusterTest", zkClient));
    
    command = "-zkSvr localhost:2183 -dropCluster \\ClusterTest";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -dropCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -dropCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    Assert.assertFalse(zkClient.exists("/clusterTest"));
    Assert.assertFalse(zkClient.exists("/\\ClusterTest"));
    Assert.assertFalse(zkClient.exists("/clusterTest1"));
    

    command = "-zkSvr localhost:2183 -addCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    /** Add / drop some resources */
    
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_22 44 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_11 44 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // Add duplicate resource 
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_22 55 OnlineOffline";
    assertException(command);
    
    // drop resource now
    command = "-zkSvr localhost:2183 -dropResource clusterTest1 db_11 ";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_11 44 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    
    /** Add / delete  instances */
    for(int i = 0; i < 3; i++)
    {
      command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:123"+i;
      ClusterSetup.processCommandLineArgs(command.split(" "));
    }
    command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:1233;localhost:1234;localhost:1235;localhost:1236";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // delete one node without disable
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:1236";
    assertException(command);
    
    // delete non-exist node
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:12367";
    assertException(command);
    
    // disable node
    command = "-zkSvr localhost:2183 -enableInstance clusterTest1 localhost:1236 false";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:1236";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // add node to controller cluster
    command = "-zkSvr localhost:2183 -addNode Klazt3rz controller:9000;controller:9001";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // add a dup host
    command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:1234";
    assertException(command);
    
    // drop and add resource
    command = "-zkSvr localhost:2183 -dropResource clusterTest1 db_11 ";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_11 12 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // Rebalance resource
    command = "-zkSvr localhost:2183 -rebalance clusterTest1 db_11 3";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -dropResource clusterTest1 db_11 ";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // re-add and rebalance
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -rebalance clusterTest1 db_11 3";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    //start mock nodes
    for(int i = 0; i < 6 ; i++)
    {
      StartCMResult result =
          TestHelper.startDummyProcess(ZK_ADDR, "clusterTest1", "localhost_123"+i);
      _startCMResultMap.put("localhost_123"+i, result);
    }
    
    //start controller nodes
    for(int i = 0; i< 2; i++)
    {
      StartCMResult result =
          TestHelper.startController("Klazt3rz", "controller_900" + i, ZK_ADDR, HelixControllerMain.DISTRIBUTED);
          
      _startCMResultMap.put("controller_900" + i, result);
    }
    Thread.sleep(100);
    
    
    // activate clusters
    command = "-zkSvr localhost:2183 -activateCluster clusterTest1 Klazters true";
    assertException(command);
    
    command = "-zkSvr localhost:2183 -activateCluster clusterTest2 Klazt3rs true";
    assertException(command);
    
    command = "-zkSvr localhost:2183 -activateCluster clusterTest1 Klazt3rz true";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    Thread.sleep(500);
    // verify leader node
    HelixDataAccessor accessor = _startCMResultMap.get("controller_9001")._manager.getHelixDataAccessor();
    LiveInstance controllerLeader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    Assert.assertTrue(controllerLeader.getInstanceName().startsWith("controller_900"));
    
    accessor = _startCMResultMap.get("localhost_1232")._manager.getHelixDataAccessor();
    LiveInstance leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    Assert.assertTrue(leader.getInstanceName().startsWith("controller_900"));
    
    Thread.sleep(1000);
    
    // drop node should fail as not disabled
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:1232";
    assertException(command);
    
    // disabled node
    command = "-zkSvr localhost:2183 -enableInstance clusterTest1 localhost:1232 false";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // Cannot drop / swap
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:1232";
    assertException(command);
    
    command = "-zkSvr localhost:2183 -swapInstance clusterTest1 localhost_1232 localhost_12320";
    assertException(command);
    
    // disconnect the node
    _startCMResultMap.get("localhost_1232")._manager.disconnect();
    _startCMResultMap.get("localhost_1232")._thread.interrupt();
    
    // add new node then swap instance
    command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:12320";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -swapInstance clusterTest1 localhost_1232 localhost_12320";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    _startCMResultMap.put("localhost_12320", TestHelper.startDummyProcess(ZK_ADDR, "clusterTest1", "localhost_12320"));
    
    // expand cluster
    command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:12331;localhost:12341;localhost:12351;localhost:12361";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -expandCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // deactivate cluster
    command = "-zkSvr localhost:2183 -activateCluster clusterTest1 Klazt3rz false";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    Thread.sleep(6000);
    
    accessor = _startCMResultMap.get("localhost_1231")._manager.getHelixDataAccessor();
    String path = accessor.keyBuilder().controllerLeader().getPath();
    Assert.assertFalse(zkClient.exists(path));
    // leader node should be gone
    for(StartCMResult result : _startCMResultMap.values())
    {
      result._manager.disconnect();
      result._thread.interrupt();
    }
  }
}

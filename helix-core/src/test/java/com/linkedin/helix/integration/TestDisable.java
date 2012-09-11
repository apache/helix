package com.linkedin.helix.integration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkTestHelper;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.mock.controller.StandaloneController;
import com.linkedin.helix.mock.storage.MockParticipant;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.ClusterStateVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;


public class TestDisable extends ZkIntegrationTestBase
{

  @Test
  public void testDisableNodeCustomIS() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;
    String disableNode = "localhost_12918";

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            8, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // set ideal state to customized mode
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setIdealStateMode(IdealStateModeProperty.CUSTOMIZED.toString());
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    
    // start controller
    StandaloneController controller =
        new StandaloneController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();
    
    // start participants
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // disable localhost_12918
    String command = "--zkSvr " + ZK_ADDR +" --enableInstance " + clusterName + 
        " " + disableNode + " false";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    
    // make sure localhost_12918 is in OFFLINE state
    Map<String, Map<String, String>> expectStateMap = new HashMap<String, Map<String, String>>();
    Map<String, String> expectInstanceStateMap = new HashMap<String, String>();
    expectInstanceStateMap.put(disableNode, "OFFLINE");
    expectStateMap.put(".*", expectInstanceStateMap);
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "==");
    Assert.assertTrue(result, disableNode + " should be in OFFLINE");
    
    // re-enable localhost_12918
    command = "--zkSvr " + ZK_ADDR +" --enableInstance " + clusterName + 
        " " + disableNode + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // make sure localhost_12918 is NOT in OFFLINE state
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "!=");
    Assert.assertTrue(result, disableNode + " should NOT be in OFFLINE");
    
    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis())); 
  }
  
  @Test
  public void testDisableNodeAutoIS() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;
    String disableNode = "localhost_12919";


    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            8, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // start controller
    StandaloneController controller =
        new StandaloneController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();
    
    // start participants
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // disable localhost_12919
    String command = "--zkSvr " + ZK_ADDR +" --enableInstance " + clusterName + 
        " " + disableNode + " false";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    
    // make sure localhost_12919 is in OFFLINE state
    Map<String, Map<String, String>> expectStateMap = new HashMap<String, Map<String, String>>();
    Map<String, String> expectInstanceStateMap = new HashMap<String, String>();
    expectInstanceStateMap.put(disableNode, "OFFLINE");
    expectStateMap.put(".*", expectInstanceStateMap);
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "==");
    Assert.assertTrue(result, disableNode + " should be in OFFLINE");
    
    // re-enable localhost_12919
    command = "--zkSvr " + ZK_ADDR +" --enableInstance " + clusterName + 
        " " + disableNode + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // make sure localhost_12919 is NOT in OFFLINE state
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "!=");
    Assert.assertTrue(result, disableNode + " should NOT be in OFFLINE");
    
    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis())); 
  }
  
  @Test
  public void testDisablePartitionCustomIS() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            8, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // set ideal state to customized mode
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setIdealStateMode(IdealStateModeProperty.CUSTOMIZED.toString());
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    
    // start controller
    StandaloneController controller =
        new StandaloneController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();
    
    // start participants
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // disable [TestDB0_0, TestDB0_5] on localhost_12919
    String command = "--zkSvr " + ZK_ADDR +" --enablePartition false " + clusterName +
        " localhost_12919 TestDB0 TestDB0_0 TestDB0_5";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    
    // make sure localhost_12918 is in OFFLINE state for [TestDB0_0, TestDB0_5]
    Map<String, Map<String, String>> expectStateMap = new HashMap<String, Map<String, String>>();
    Map<String, String> expectInstanceStateMap = new HashMap<String, String>();
    expectInstanceStateMap.put("localhost_12919", "OFFLINE");
    expectStateMap.put("TestDB0_0", expectInstanceStateMap);
    expectStateMap.put("TestDB0_5", expectInstanceStateMap);
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "==");
    Assert.assertTrue(result, "localhost_12919" + " should be in OFFLINE for [TestDB0_0, TestDB0_5]");


    // re-enable localhost_12919 for [TestDB0_0, TestDB0_5]
    command = "--zkSvr " + ZK_ADDR +" --enablePartition true " + clusterName +
        " localhost_12919 TestDB0 TestDB0_0 TestDB0_5";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // make sure localhost_12919 is NOT in OFFLINE state for [TestDB0_0, TestDB0_5]
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "!=");
    Assert.assertTrue(result,  "localhost_12919" + " should NOT be in OFFLINE");

    
    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis())); 
  }
  
  @Test
  public void testDisablePartitionAutoIS() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                            "localhost", // participant name prefix
                            "TestDB", // resource name prefix
                            1, // resources
                            8, // partitions per resource
                            n, // number of nodes
                            3, // replicas
                            "MasterSlave",
                            true); // do rebalance

    // start controller
    StandaloneController controller =
        new StandaloneController(clusterName, "controller_0", ZK_ADDR);
    controller.syncStart();
    
    // start participants
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // disable [TestDB0_0, TestDB0_5] on localhost_12919
    String command = "--zkSvr " + ZK_ADDR +" --enablePartition false " + clusterName +
        " localhost_12919 TestDB0 TestDB0_0 TestDB0_5";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    
    // make sure localhost_12918 is in OFFLINE state for [TestDB0_0, TestDB0_5]
    Map<String, Map<String, String>> expectStateMap = new HashMap<String, Map<String, String>>();
    Map<String, String> expectInstanceStateMap = new HashMap<String, String>();
    expectInstanceStateMap.put("localhost_12919", "OFFLINE");
    expectStateMap.put("TestDB0_0", expectInstanceStateMap);
    expectStateMap.put("TestDB0_5", expectInstanceStateMap);
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "==");
    Assert.assertTrue(result, "localhost_12919" + " should be in OFFLINE for [TestDB0_0, TestDB0_5]");


    // re-enable localhost_12919 for [TestDB0_0, TestDB0_5]
    command = "--zkSvr " + ZK_ADDR +" --enablePartition true " + clusterName +
        " localhost_12919 TestDB0 TestDB0_0 TestDB0_5";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    // make sure localhost_12919 is NOT in OFFLINE state for [TestDB0_0, TestDB0_5]
    result = ZkTestHelper.verifyState(_gZkClient, clusterName, "TestDB0", expectStateMap, "!=");
    Assert.assertTrue(result,  "localhost_12919" + " should NOT be in OFFLINE");

    
    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < 5; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis())); 
  }

}

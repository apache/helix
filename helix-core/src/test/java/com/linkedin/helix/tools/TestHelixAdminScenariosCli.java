package com.linkedin.helix.tools;
/*
 * Simulate all the admin tasks needed by using command line tool
 * 
 * */
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.controller.restlet.ZKPropertyTransferServer;
import com.linkedin.helix.controller.restlet.ZkPropertyTransferClient;
import com.linkedin.helix.integration.ZkIntegrationTestBase;
import com.linkedin.helix.manager.zk.ZKUtil;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import com.linkedin.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;

public class TestHelixAdminScenariosCli extends ZkIntegrationTestBase
{
  Map<String, StartCMResult> _startCMResultMap =
        new HashMap<String, StartCMResult>();
  
  public static String ObjectToJson(Object object) throws JsonGenerationException,
    JsonMappingException,
    IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    SerializationConfig serializationConfig = mapper.getSerializationConfig();
    serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, true);
    
    StringWriter sw = new StringWriter();
    mapper.writeValue(sw, object);
    
    return sw.toString();
  }

  
  @Test
  public void TestAddDeleteClusterAndInstanceAndResource() throws Exception
  {
    // Helix bug helix-102
    //ZKPropertyTransferServer.PERIOD = 500;
    //ZkPropertyTransferClient.SEND_PERIOD = 500;
    //ZKPropertyTransferServer.getInstance().init(19999, ZK_ADDR);
    
    /**======================= Add clusters ==============================*/
    
    testAddCluster();
    
    /**================= Add / drop some resources ===========================*/
    
    testAddResource();
    
    /**====================== Add / delete  instances ===========================*/
    
    testAddInstance();
    
    /**===================== Rebalance resource ===========================*/
    
    testRebalanceResource();
    
    /**====================  start the clusters =============================*/
    
    testStartCluster();
    
    /**====================  drop add resource in live clusters ===================*/
    testDropAddResource();
    /**======================Operations with live node ============================*/
   
    testInstanceOperations();
    
    /**============================ expand cluster ===========================*/
   
    testExpandCluster();
      
    /**============================ deactivate cluster ===========================*/
    testDeactivateCluster();
    
  }
  
  void assertClusterSetupException(String command)
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
  
  public void testAddCluster() throws Exception
  {
    String command = "--zkSvr localhost:2183 -addCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // malformed cluster name 
    command = "--zkSvr localhost:2183 -addCluster /ClusterTest";
    assertClusterSetupException(command);
    
    // Add the grand cluster
    command = "--zkSvr localhost:2183 -addCluster \"Klazt3rz";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "--zkSvr localhost:2183 -addCluster \\ClusterTest";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // Add already exist cluster
    command = "--zkSvr localhost:2183 -addCluster clusterTest";
    assertClusterSetupException(command);
    
    // delete cluster without resource and instance
    Assert.assertTrue(ZKUtil.isClusterSetup("Klazt3rz", _gZkClient));
    Assert.assertTrue(ZKUtil.isClusterSetup("clusterTest", _gZkClient));
    Assert.assertTrue(ZKUtil.isClusterSetup("\\ClusterTest", _gZkClient));
    
    command = "-zkSvr localhost:2183 -dropCluster \\ClusterTest";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -dropCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -dropCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    Assert.assertFalse(_gZkClient.exists("/clusterTest"));
    Assert.assertFalse(_gZkClient.exists("/\\ClusterTest"));
    Assert.assertFalse(_gZkClient.exists("/clusterTest1"));
    
    command = "-zkSvr localhost:2183 -addCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split(" "));
  }
  
  public void testAddResource() throws Exception
  {
    String command = "-zkSvr localhost:2183 -addResource clusterTest1 db_22 144 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_11 44 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // Add duplicate resource 
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_22 55 OnlineOffline";
    assertClusterSetupException(command);
    
    // drop resource now
    command = "-zkSvr localhost:2183 -dropResource clusterTest1 db_11 ";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_11 44 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
  }

  private void testDeactivateCluster() throws Exception, InterruptedException
  {
    String command;
    HelixDataAccessor accessor;
    String path;
    // deactivate cluster
    command = "-zkSvr localhost:2183 -activateCluster clusterTest1 Klazt3rz false";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    Thread.sleep(6000);
    
    accessor = _startCMResultMap.get("localhost_1231")._manager.getHelixDataAccessor();
    path = accessor.keyBuilder().controllerLeader().getPath();
    Assert.assertFalse(_gZkClient.exists(path));
    
    command = "-zkSvr localhost:2183 -dropCluster clusterTest1";
    assertClusterSetupException(command);
    
    // leader node should be gone
    for(StartCMResult result : _startCMResultMap.values())
    {
      result._manager.disconnect();
      result._thread.interrupt();
    }

    command = "-zkSvr localhost:2183 -dropCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -dropCluster Klazt3rz";
    ClusterSetup.processCommandLineArgs(command.split(" "));
  }
  
  private void testDropAddResource() throws Exception
  {
    ZNRecord record = _gSetupTool._admin.getResourceIdealState("clusterTest1", "db_11").getRecord();
    String x = ObjectToJson(record);
    
    FileWriter fos = new FileWriter("/tmp/temp.log"); 
    PrintWriter pw = new PrintWriter(fos);
    pw.write(x);
    pw.close();
    
    String command = "-zkSvr localhost:2183 -dropResource clusterTest1 db_11 ";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);
    
    command = "-zkSvr localhost:2183 -addIdealState clusterTest1 db_11 \"/tmp/temp.log\"";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);
    
    ZNRecord record2 = _gSetupTool._admin.getResourceIdealState("clusterTest1", "db_11").getRecord();
    Assert.assertTrue(record2.equals(record));
  }

  private void testExpandCluster() throws Exception
  {
    String command;
    HelixDataAccessor accessor;
    boolean verifyResult;
    String path;
    
    command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:12331;localhost:12341;localhost:12351;localhost:12361";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -expandCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    for(int i = 3; i<= 6; i++)
    {
      StartCMResult result =
          TestHelper.startDummyProcess(ZK_ADDR, "clusterTest1", "localhost_123"+i+"1");
      _startCMResultMap.put("localhost_123"+i + "1", result);
    }
    
    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              "clusterTest1"));
    Assert.assertTrue(verifyResult);
    
    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);
  }

  private void testInstanceOperations() throws Exception
  {
    String command;
    HelixDataAccessor accessor;
    boolean verifyResult;
    // drop node should fail as not disabled
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:1232";
    assertClusterSetupException(command);
    
    // disabled node
    command = "-zkSvr localhost:2183 -enableInstance clusterTest1 localhost:1232 false";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // Cannot drop / swap
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:1232";
    assertClusterSetupException(command);
    
    command = "-zkSvr localhost:2183 -swapInstance clusterTest1 localhost_1232 localhost_12320";
    assertClusterSetupException(command);
    
    // disconnect the node
    _startCMResultMap.get("localhost_1232")._manager.disconnect();
    _startCMResultMap.get("localhost_1232")._thread.interrupt();
    
    // add new node then swap instance
    command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:12320";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // swap instance. The instance get swapped out should not exist anymore
    command = "-zkSvr localhost:2183 -swapInstance clusterTest1 localhost_1232 localhost_12320";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    accessor = _startCMResultMap.get("localhost_1231")._manager.getHelixDataAccessor();
    String path = accessor.keyBuilder().instanceConfig("localhost_1232").getPath();
    Assert.assertFalse(_gZkClient.exists(path));
    
    _startCMResultMap.put("localhost_12320", TestHelper.startDummyProcess(ZK_ADDR, "clusterTest1", "localhost_12320"));
  }

  private void testStartCluster() throws Exception, InterruptedException
  {
    String command;
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
    // wrong grand clustername
    command = "-zkSvr localhost:2183 -activateCluster clusterTest1 Klazters true";
    assertClusterSetupException(command);
    
    // wrong cluster name
    command = "-zkSvr localhost:2183 -activateCluster clusterTest2 Klazt3rs true";
    assertClusterSetupException(command);
    
    command = "-zkSvr localhost:2183 -activateCluster clusterTest1 Klazt3rz true";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    Thread.sleep(500);
    
    command = "-zkSvr localhost:2183 -dropCluster clusterTest1";
    assertClusterSetupException(command);
    
    // verify leader node
    HelixDataAccessor accessor = _startCMResultMap.get("controller_9001")._manager.getHelixDataAccessor();
    LiveInstance controllerLeader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    Assert.assertTrue(controllerLeader.getInstanceName().startsWith("controller_900"));
    
    accessor = _startCMResultMap.get("localhost_1232")._manager.getHelixDataAccessor();
    LiveInstance leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    Assert.assertTrue(leader.getInstanceName().startsWith("controller_900"));
    
    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR,
                                                                              "clusterTest1"));
    Assert.assertTrue(verifyResult);
    
    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 "clusterTest1"));
    Assert.assertTrue(verifyResult);
  }

  private void testRebalanceResource() throws Exception
  {
    String command = "-zkSvr localhost:2183 -rebalance clusterTest1 db_11 3";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -dropResource clusterTest1 db_11 ";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // re-add and rebalance
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -rebalance clusterTest1 db_11 3";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // rebalance with key prefix
    command = "-zkSvr localhost:2183 -rebalance clusterTest1 db_22 2 -key alias";
    ClusterSetup.processCommandLineArgs(command.split(" "));
  }

  private void testAddInstance() throws Exception
  {
    String command;
    for(int i = 0; i < 3; i++)
    {
      command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:123"+i;
      ClusterSetup.processCommandLineArgs(command.split(" "));
    }
    command = "-zkSvr localhost:2183 -addNode clusterTest1 localhost:1233;localhost:1234;localhost:1235;localhost:1236";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    // delete one node without disable
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:1236";
    assertClusterSetupException(command);
    
    // delete non-exist node
    command = "-zkSvr localhost:2183 -dropNode clusterTest1 localhost:12367";
    assertClusterSetupException(command);
    
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
    assertClusterSetupException(command);
    
    // drop and add resource
    command = "-zkSvr localhost:2183 -dropResource clusterTest1 db_11 ";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    
    command = "-zkSvr localhost:2183 -addResource clusterTest1 db_11 12 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split(" "));
  }
}

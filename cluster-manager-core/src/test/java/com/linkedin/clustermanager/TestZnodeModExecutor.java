package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ZnodeModCommand;
import com.linkedin.clustermanager.tools.ZnodeModDesc;
import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;
import com.linkedin.clustermanager.tools.ZnodeModExecutor;
import com.linkedin.clustermanager.tools.ZnodeModVerifier;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestZnodeModExecutor
{
  private static Logger logger = Logger.getLogger(TestZnodeModExecutor.class);
  private static final String ZK_ADDR = "localhost:2181";
  private final String PREFIX = "/" + getShortClassName();
  private ZkServer _zkServer = null; 

  @Test
  public void testBasic() throws Exception
  {
    logger.info("RUN: " + new Date(System.currentTimeMillis()));
    
    // test case for the basic flow, no timeout, no data trigger
    ZnodeModDesc testDesc = new ZnodeModDesc("testBasic");
    String pathChild1 = PREFIX + "/basic_child1";
    String pathChild2 = PREFIX + "/basic_child2";
    
    ZnodeModCommand command; 
    command = new ZnodeModCommand(pathChild1, ZnodePropertyType.SIMPLE, "+", "key1", "simpleValue1");
    testDesc.addCommand(command);
    
    List<String> list = new ArrayList<String>();
    list.add("listValue1");
    list.add("listValue2");
    command = new ZnodeModCommand(pathChild1, ZnodePropertyType.LIST, "+", "key2", list);
    testDesc.addCommand(command);
    
    ZNRecord record = getExampleZNRecord();
    command = new ZnodeModCommand(pathChild2, ZnodePropertyType.ZNODE, "+", record);
    testDesc.addCommand(command);
    
    ZnodeModVerifier verifier;
    verifier = new ZnodeModVerifier(pathChild1, ZnodePropertyType.SIMPLE, "==", "key1", "simpleValue1"); 
    testDesc.addVerification(verifier);
    
    verifier = new ZnodeModVerifier(pathChild1, ZnodePropertyType.LIST, "==", "key2", list); 
    testDesc.addVerification(verifier);
    
    verifier = new ZnodeModVerifier(pathChild2, ZnodePropertyType.ZNODE, "==", record); 
    testDesc.addVerification(verifier);
    
    ZnodeModExecutor executor = new ZnodeModExecutor(testDesc, ZK_ADDR);
    Map<String, Boolean> results = executor.executeTest();
    for (ZnodeModCommand resultCommand : testDesc.getCommands())
    {
      Assert.assertTrue(results.get(resultCommand.toString()).booleanValue());
    }
    for (ZnodeModVerifier resultVerifier : testDesc.getVerifiers())
    {
      Assert.assertTrue(results.get(resultVerifier.toString()).booleanValue());
    } 
    
    logger.info("END: " + new Date(System.currentTimeMillis()));
  }
  
  @Test
  public void testDataTrigger() throws Exception
  {
    logger.info("RUN: " + new Date(System.currentTimeMillis()));
    
    // test case for data trigger, no timeout
    ZnodeModDesc testDesc = new ZnodeModDesc("testDataTrigger");
    String pathChild1 = PREFIX + "/data_trigger_child1";
    String pathChild2 = PREFIX + "/data_trigger_child2";
    
    ZnodeModCommand command = new ZnodeModCommand(pathChild1, ZnodePropertyType.SIMPLE, 
                                  "+", "key1", "simpleValue1", "simpleValue1-new");
    testDesc.addCommand(command);
    
    ZNRecord record = getExampleZNRecord();
    ZNRecord recordNew = new ZNRecord(record);
    recordNew.setSimpleField("ideal_state_mode", IdealStateConfigProperty.AUTO.toString());
    command = new ZnodeModCommand(pathChild2, ZnodePropertyType.ZNODE, 
                                  "+", record, recordNew);
    testDesc.addCommand(command);
    
    ZnodeModVerifier verifier = new ZnodeModVerifier(pathChild1, ZnodePropertyType.SIMPLE, 
                                    "==", "key1", "simpleValue1-new"); 
    testDesc.addVerification(verifier);
    verifier = new ZnodeModVerifier(pathChild2, ZnodePropertyType.ZNODE, 
                                                     "==", recordNew); 
    testDesc.addVerification(verifier);
    
    ZnodeModExecutor executor = new ZnodeModExecutor(testDesc, ZK_ADDR);
    Map<String, Boolean> results = executor.executeTest();
    for (ZnodeModCommand resultCommand : testDesc.getCommands())
    {
      Assert.assertFalse(results.get(resultCommand.toString()).booleanValue());
    }
    for (ZnodeModVerifier resultVerifier : testDesc.getVerifiers())
    {
      Assert.assertFalse(results.get(resultVerifier.toString()).booleanValue());
    }

    logger.info("END: " + new Date(System.currentTimeMillis()));
  }
  
  @Test
  public void testTimeout() throws Exception
  {
    logger.info("RUN: " + new Date(System.currentTimeMillis()));
    
    // test case for timeout, no data trigger
    ZnodeModDesc testDesc = new ZnodeModDesc("testTimeout");
    String pathChild1 = PREFIX + "/timeout_child1";
    String pathChild2 = PREFIX + "/timeout_child2";

    ZnodeModCommand command = new ZnodeModCommand(0, 1000, pathChild1, ZnodePropertyType.SIMPLE, 
                        "+", "key1", "simpleValue1", "simpleValue1-new");
    testDesc.addCommand(command);
    
    ZNRecord record = getExampleZNRecord();
    ZNRecord recordNew = new ZNRecord(record);
    recordNew.setSimpleField("ideal_state_mode", IdealStateConfigProperty.AUTO.toString());
    command = new ZnodeModCommand(0, 500, pathChild2, ZnodePropertyType.ZNODE, 
                                  "+", record, recordNew);
    testDesc.addCommand(command);
                              
    ZnodeModVerifier verifier = new ZnodeModVerifier(1000, pathChild1, ZnodePropertyType.SIMPLE, 
                                    "==", "key1", "simpleValue1-new");
    testDesc.addVerification(verifier);
    verifier = new ZnodeModVerifier(pathChild2, ZnodePropertyType.ZNODE, 
                                    "==", recordNew); 
    testDesc.addVerification(verifier);
    
    ZnodeModExecutor executor = new ZnodeModExecutor(testDesc, ZK_ADDR);
    Map<String, Boolean> results = executor.executeTest();
    for (ZnodeModCommand resultCommand : testDesc.getCommands())
    {
      Assert.assertFalse(results.get(resultCommand.toString()).booleanValue());
    }
    for (ZnodeModVerifier resultVerifier : testDesc.getVerifiers())
    {
      Assert.assertFalse(results.get(resultVerifier.toString()).booleanValue());
    }

    logger.info("END: " + new Date(System.currentTimeMillis()));
  }
  
  @Test
  public void testDataTriggerWithTimeout() throws Exception
  {
    logger.info("RUN: " + new Date(System.currentTimeMillis()));
    
    // test case for data trigger with timeout
    ZnodeModDesc testDesc = new ZnodeModDesc("testDataTriggerWithTimeout");
    final String pathChild1 = PREFIX + "/dataTriggerWithTimeout_child1";
    
    final ZNRecord record = getExampleZNRecord();
    ZNRecord recordNew = new ZNRecord(record);
    recordNew.setSimpleField("ideal_state_mode", IdealStateConfigProperty.AUTO.toString());
    ZnodeModCommand command = new ZnodeModCommand(0, 10000, pathChild1, ZnodePropertyType.ZNODE, 
                        "+", record, recordNew);
    testDesc.addCommand(command);
    ZnodeModVerifier verifier = new ZnodeModVerifier(1000, pathChild1, ZnodePropertyType.ZNODE, 
                                    "==", recordNew); 
    testDesc.addVerification(verifier);
    
    // start a separate thread to change znode at pathChild1
    new Thread()
    {
      @Override
      public void run()
      {
        try
        {
          Thread.sleep(3000);
          ZkClient zkClient = ZKClientPool.getZkClient(ZK_ADDR);
          zkClient.createPersistent(pathChild1, true);
          zkClient.writeData(pathChild1, record);
        }
        catch (InterruptedException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }.start();
    
    ZnodeModExecutor executor = new ZnodeModExecutor(testDesc, ZK_ADDR);
    Map<String, Boolean> results = executor.executeTest();
    for (ZnodeModCommand resultCommand : testDesc.getCommands())
    {
      Assert.assertTrue(results.get(resultCommand.toString()).booleanValue());
    }
    for (ZnodeModVerifier resultVerifier : testDesc.getVerifiers())
    {
      Assert.assertTrue(results.get(resultVerifier.toString()).booleanValue());
    }
    
  }
  
  @BeforeTest
  public void beforeTest()
  {
    _zkServer = TestHelper.startZkSever(ZK_ADDR, PREFIX);
    
  }
  
  @AfterTest
  public void afterTest()
  {
    TestHelper.stopZkServer(_zkServer);
  }
  
  private ZNRecord getExampleZNRecord()
  {
    ZNRecord record = new ZNRecord("example");
    record.setSimpleField("ideal_state_mode", IdealStateConfigProperty.CUSTOMIZED.toString());
    Map<String, String> map = new HashMap<String, String>();
    map.put("localhost_12918", "MASTER");
    map.put("localhost_12919", "SLAVE");
    record.setMapField("TestDB_0", map);
    
    List<String> list = new ArrayList<String>();
    list.add("localhost_12918");
    list.add("localhost_12919");
    record.setListField("TestDB_0", list);
    return record;
  }
  
  private String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }
}

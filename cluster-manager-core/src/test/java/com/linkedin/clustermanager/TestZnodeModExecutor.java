package com.linkedin.clustermanager;

import java.util.Date;
import java.util.Map;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.tools.ZnodeModCommand;
import com.linkedin.clustermanager.tools.ZnodeModDesc;
import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;
import com.linkedin.clustermanager.tools.ZnodeModExecutor;
import com.linkedin.clustermanager.tools.ZnodeModVerifier;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestZnodeModExecutor
{
  private final String _zkAdress = "localhost:2191";
  private final String _znodePath = "/TEST_CASES";
  private ZkServer _zkServer = null;

  @Test
  public void test() throws Exception
  {
    System.out.println("Running TestZnodeModExecutor: " + new Date(System.currentTimeMillis()));
    
    // test case for the basic flow
    ZnodeModDesc testDesc = new ZnodeModDesc("test1");
    ZnodeModCommand command = new ZnodeModCommand(_znodePath, ZnodePropertyType.SIMPLE, 
                                                  "+", "key1", null, "value1_1");
    testDesc.addCommand(command);
    ZnodeModCommand command2 = new ZnodeModCommand(_znodePath, ZnodePropertyType.SIMPLE, 
                                  "+", "key5", null, "value5_1");
    testDesc.addCommand(command2);
    
    ZnodeModVerifier verifier = new ZnodeModVerifier(_znodePath, ZnodePropertyType.SIMPLE, 
                                                     "==", "key1", "value1_1"); 
    testDesc.addVerification(verifier);
    ZnodeModVerifier verifier2 = new ZnodeModVerifier(_znodePath, ZnodePropertyType.SIMPLE, 
                                    "==", "key5", "value5_1"); 
    testDesc.addVerification(verifier2);
    
    ZnodeModExecutor executor = new ZnodeModExecutor(testDesc, _zkAdress);
    Map<String, Boolean> results = executor.executeTest();
    Assert.assertEquals(true, results.get(command.toString()).booleanValue());
    Assert.assertEquals(true, results.get(command2.toString()).booleanValue());
    Assert.assertEquals(true, results.get(verifier.toString()).booleanValue());
    Assert.assertEquals(true, results.get(verifier2.toString()).booleanValue());
    
    // test case for command/verifier fails
    testDesc = new ZnodeModDesc("test2");
    command = new ZnodeModCommand(_znodePath, ZnodePropertyType.SIMPLE, 
                                  "+", "key2", "value2_0", "value2_1");
    testDesc.addCommand(command);
    verifier = new ZnodeModVerifier(_znodePath, ZnodePropertyType.SIMPLE, 
                                    "==", "key2", "value2_1"); 
    testDesc.addVerification(verifier);
    
    executor = new ZnodeModExecutor(testDesc, _zkAdress);
    results = executor.executeTest();
    Assert.assertEquals(false, results.get(command.toString()).booleanValue());
    Assert.assertEquals(false, results.get(verifier.toString()).booleanValue());
    
    // test case for command/verifier fails with timeout
    testDesc = new ZnodeModDesc("test3");
    command = new ZnodeModCommand(0, 1000, _znodePath, ZnodePropertyType.SIMPLE, 
                                  "+", "key3", "value3_0", "value3_1");
    testDesc.addCommand(command);
    verifier = new ZnodeModVerifier(1000, _znodePath, ZnodePropertyType.SIMPLE, 
                                    "==", "key3", "value3_1"); 
    testDesc.addVerification(verifier);
    
    executor = new ZnodeModExecutor(testDesc, _zkAdress);
    results = executor.executeTest();
    Assert.assertEquals(false, results.get(command.toString()).booleanValue());
    Assert.assertEquals(false, results.get(verifier.toString()).booleanValue());

    // test case for command with data trigger, and verifier
    testDesc = new ZnodeModDesc("test4");
    command = new ZnodeModCommand(0, 10000, _znodePath, ZnodePropertyType.SIMPLE, 
                                  "+", "key4", "value4_0", "value4_1");
    testDesc.addCommand(command);
    verifier = new ZnodeModVerifier(1000, _znodePath, ZnodePropertyType.SIMPLE, 
                                    "==", "key4", "value4_1"); 
    testDesc.addVerification(verifier);
    
    // start a separate thread to change SIMPLE/key4 to value4_0
    new Thread()
    {
      @Override
      public void run()
      {
        try
        {
          Thread.sleep(3000);
          ZkClient zkClient = ZKClientPool.getZkClient(_zkAdress);
          ZNRecord record = new ZNRecord();
          record.setSimpleField("key4", "value4_0");
          zkClient.writeData(_znodePath, record);
        }
        catch (InterruptedException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }.start();
    
    executor = new ZnodeModExecutor(testDesc, _zkAdress);
    results = executor.executeTest();
    Assert.assertEquals(true, results.get(command.toString()).booleanValue());
    Assert.assertEquals(true, results.get(verifier.toString()).booleanValue());
    
    System.out.println("Ending TestZnodeModExecutor: " + new Date(System.currentTimeMillis()));

  }
  
  @BeforeTest
  public void beforeTest()
  {
    _zkServer = TestHelper.startZkSever(_zkAdress, _znodePath);
    
  }
  
  @AfterTest
  public void afterTest()
  {
    TestHelper.stopZkServer(_zkServer);
  }
}

package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import com.linkedin.clustermanager.tools.ZnodeModValue;
import com.linkedin.clustermanager.tools.ZnodeModVerifier;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestZnodeModExecutor
{
  private final String _zkAddress = "localhost:2191";
  private final String _znodePath = "/testPath";
  private ZkServer _zkServer = null;

  @Test
  public void test() throws Exception
  {
    System.out.println("Running TestZnodeModExecutor: " + new Date(System.currentTimeMillis()));
    
    // test case for the basic flow, no timeout, no data trigger
    ZnodeModDesc testDesc = new ZnodeModDesc("test1");
    ZnodeModCommand command = new ZnodeModCommand(_znodePath, ZnodePropertyType.SIMPLE, 
                                                  "+", "key1", null, new ZnodeModValue("value1_1"));
    testDesc.addCommand(command);
    
    List<String> list = new ArrayList<String>();
    list.add("value5_1");
    list.add("value5_2");
    ZnodeModCommand command2 = new ZnodeModCommand(_znodePath, ZnodePropertyType.LIST, 
                                                   "+", "key5", null, new ZnodeModValue(list));
    testDesc.addCommand(command2);
    
    ZnodeModVerifier verifier = new ZnodeModVerifier(_znodePath, ZnodePropertyType.SIMPLE, 
                                                     "==", "key1", new ZnodeModValue("value1_1")); 
    testDesc.addVerification(verifier);
    
    
    ZnodeModVerifier verifier2 = new ZnodeModVerifier(_znodePath, ZnodePropertyType.LIST, 
                                                      "==", "key5", new ZnodeModValue(list)); 
    testDesc.addVerification(verifier2);
    
    ZnodeModExecutor executor = new ZnodeModExecutor(testDesc, _zkAddress);
    Map<String, Boolean> results = executor.executeTest();
    Assert.assertEquals(true, results.get(command.toString()).booleanValue());
    Assert.assertEquals(true, results.get(command2.toString()).booleanValue());
    Assert.assertEquals(true, results.get(verifier.toString()).booleanValue());
    Assert.assertEquals(true, results.get(verifier2.toString()).booleanValue());
    
    
    // test case for command/verifier fails on data-trigger
    testDesc = new ZnodeModDesc("test2");
    command = new ZnodeModCommand(_znodePath, ZnodePropertyType.SIMPLE, 
                                  "+", "key2", new ZnodeModValue("value2_0"), new ZnodeModValue("value2_1"));
    testDesc.addCommand(command);
    verifier = new ZnodeModVerifier(_znodePath, ZnodePropertyType.SIMPLE, 
                                    "==", "key2", new ZnodeModValue("value2_1")); 
    testDesc.addVerification(verifier);
    
    executor = new ZnodeModExecutor(testDesc, _zkAddress);
    results = executor.executeTest();
    Assert.assertEquals(false, results.get(command.toString()).booleanValue());
    Assert.assertEquals(false, results.get(verifier.toString()).booleanValue());
    
    // test case for command/verifier fails on timeout
    testDesc = new ZnodeModDesc("test3");
    command = new ZnodeModCommand(0, 1000, _znodePath, ZnodePropertyType.SIMPLE, 
                                  "+", "key3", new ZnodeModValue("value3_0"), new ZnodeModValue("value3_1"));
    testDesc.addCommand(command);
    verifier = new ZnodeModVerifier(1000, _znodePath, ZnodePropertyType.SIMPLE, 
                                    "==", "key3", new ZnodeModValue("value3_1")); 
    testDesc.addVerification(verifier);
    
    executor = new ZnodeModExecutor(testDesc, _zkAddress);
    results = executor.executeTest();
    Assert.assertEquals(false, results.get(command.toString()).booleanValue());
    Assert.assertEquals(false, results.get(verifier.toString()).booleanValue());

    // test case for command with data trigger, and verifier
    testDesc = new ZnodeModDesc("test4");
    command = new ZnodeModCommand(0, 10000, _znodePath, ZnodePropertyType.SIMPLE, 
                                  "+", "key4", new ZnodeModValue("value4_0"), new ZnodeModValue("value4_1"));
    testDesc.addCommand(command);
    verifier = new ZnodeModVerifier(1000, _znodePath, ZnodePropertyType.SIMPLE, 
                                    "==", "key4", new ZnodeModValue("value4_1")); 
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
          ZkClient zkClient = ZKClientPool.getZkClient(_zkAddress);
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
    
    executor = new ZnodeModExecutor(testDesc, _zkAddress);
    results = executor.executeTest();
    Assert.assertEquals(true, results.get(command.toString()).booleanValue());
    Assert.assertEquals(true, results.get(verifier.toString()).booleanValue());
    
    System.out.println("Ending TestZnodeModExecutor: " + new Date(System.currentTimeMillis()));
    
  }
  
  @BeforeTest
  public void beforeTest()
  {
    _zkServer = TestHelper.startZkSever(_zkAddress, _znodePath);
    
  }
  
  @AfterTest
  public void afterTest()
  {
    TestHelper.stopZkServer(_zkServer);
  }
}

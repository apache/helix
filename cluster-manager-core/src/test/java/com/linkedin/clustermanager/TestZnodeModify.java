package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.TestCommand;
import com.linkedin.clustermanager.tools.TestCommand.CommandType;
import com.linkedin.clustermanager.tools.TestExecutor;
import com.linkedin.clustermanager.tools.TestExecutor.ZnodePropertyType;
import com.linkedin.clustermanager.tools.TestTrigger;
import com.linkedin.clustermanager.tools.ZnodeOpArg;

public class TestZnodeModify extends ZkUnitTestBase
{
  private static Logger logger = Logger.getLogger(TestZnodeModify.class);
  private final String PREFIX = "/" + getShortClassName();

  @Test ()
  public void testBasic() throws Exception
  {
    logger.info("RUN: " + new Date(System.currentTimeMillis()));
    List<TestCommand> commandList = new ArrayList<TestCommand>();

    // test case for the basic flow, no timeout, no data trigger
    String pathChild1 = PREFIX + "/basic_child1";
    String pathChild2 = PREFIX + "/basic_child2";

    TestCommand command;
    ZnodeOpArg arg;
    arg = new ZnodeOpArg(pathChild1, ZnodePropertyType.SIMPLE, "+", "key1", "simpleValue1");
    command = new TestCommand(CommandType.MODIFY, arg);
    commandList.add(command);

    List<String> list = new ArrayList<String>();
    list.add("listValue1");
    list.add("listValue2");
    arg = new ZnodeOpArg(pathChild1, ZnodePropertyType.LIST, "+", "key2", list);
    command = new TestCommand(CommandType.MODIFY, arg);
    commandList.add(command);

    ZNRecord record = getExampleZNRecord();
    arg = new ZnodeOpArg(pathChild2, ZnodePropertyType.ZNODE, "+", record);
    command = new TestCommand(CommandType.MODIFY, arg);
    commandList.add(command);

    arg = new ZnodeOpArg(pathChild1, ZnodePropertyType.SIMPLE, "==", "key1");
    command = new TestCommand(CommandType.VERIFY, new TestTrigger(100, 0, "simpleValue1"), arg);
    commandList.add(command);

    arg = new ZnodeOpArg(pathChild1, ZnodePropertyType.LIST, "==", "key2");
    command = new TestCommand(CommandType.VERIFY, new TestTrigger(100, 0, list), arg);
    commandList.add(command);

    arg = new ZnodeOpArg(pathChild2, ZnodePropertyType.ZNODE, "==");
    command = new TestCommand(CommandType.VERIFY, new TestTrigger(100, 0, record), arg);
    commandList.add(command);

    Map<TestCommand, Boolean> results = TestExecutor.executeTest(commandList, ZK_ADDR);
    for (Map.Entry<TestCommand, Boolean> entry : results.entrySet())
    {
      Assert.assertTrue(entry.getValue());
    }

    logger.info("END: " + new Date(System.currentTimeMillis()));
  }

  @Test ()
  public void testDataTrigger() throws Exception
  {
    logger.info("RUN: " + new Date(System.currentTimeMillis()));
    List<TestCommand> commandList = new ArrayList<TestCommand>();

    // test case for data trigger, no timeout
    String pathChild1 = PREFIX + "/data_trigger_child1";
    String pathChild2 = PREFIX + "/data_trigger_child2";

    ZnodeOpArg arg;
    TestCommand command;

    ZnodeOpArg arg1 = new ZnodeOpArg(pathChild1, ZnodePropertyType.SIMPLE, "+", "key1", "simpleValue1-new");
    TestCommand command1 = new TestCommand(CommandType.MODIFY, new TestTrigger(0, 0, "simpleValue1"), arg1);
    commandList.add(command1);

    ZNRecord record = getExampleZNRecord();
    ZNRecord recordNew = new ZNRecord(record);
    recordNew.setSimpleField("ideal_state_mode", IdealStateConfigProperty.AUTO.toString());
    arg = new ZnodeOpArg(pathChild2, ZnodePropertyType.ZNODE, "+", recordNew);
    command = new TestCommand(CommandType.MODIFY, new TestTrigger(0, 3000, record), arg);
    commandList.add(command);

    arg = new ZnodeOpArg(pathChild2, ZnodePropertyType.ZNODE, "+", record);
    command = new TestCommand(CommandType.MODIFY, new TestTrigger(1000), arg);
    commandList.add(command);

    arg = new ZnodeOpArg(pathChild1, ZnodePropertyType.SIMPLE, "!=", "key1");
    command = new TestCommand(CommandType.VERIFY, new TestTrigger(3100, 0, "simpleValue1-new"), arg);
    commandList.add(command);

    arg = new ZnodeOpArg(pathChild2, ZnodePropertyType.ZNODE, "==");
    command = new TestCommand(CommandType.VERIFY, new TestTrigger(3100, 0, recordNew), arg);
    commandList.add(command);

    Map<TestCommand, Boolean> results = TestExecutor.executeTest(commandList, ZK_ADDR);

    boolean result = results.remove(command1).booleanValue();
    AssertJUnit.assertFalse(result);
    for (Map.Entry<TestCommand, Boolean> entry : results.entrySet())
    {
      Assert.assertTrue(entry.getValue());
    }

    logger.info("END: " + new Date(System.currentTimeMillis()));
  }

  @Test ()
  public void testTimeout() throws Exception
  {
    logger.info("RUN: " + new Date(System.currentTimeMillis()));
    List<TestCommand> commandList = new ArrayList<TestCommand>();

    // test case for timeout, no data trigger
    String pathChild1 = PREFIX + "/timeout_child1";
    String pathChild2 = PREFIX + "/timeout_child2";

    ZnodeOpArg arg1 = new ZnodeOpArg(pathChild1, ZnodePropertyType.SIMPLE, "+", "key1", "simpleValue1-new");
    TestCommand command1 = new TestCommand(CommandType.MODIFY, new TestTrigger(0, 1000, "simpleValue1"), arg1);
    commandList.add(command1);

    ZNRecord record = getExampleZNRecord();
    ZNRecord recordNew = new ZNRecord(record);
    recordNew.setSimpleField("ideal_state_mode", IdealStateConfigProperty.AUTO.toString());
    arg1 = new ZnodeOpArg(pathChild2, ZnodePropertyType.ZNODE, "+", recordNew);
    command1 = new TestCommand(CommandType.MODIFY, new TestTrigger(0, 500, record), arg1);
    commandList.add(command1);

    arg1 = new ZnodeOpArg(pathChild1, ZnodePropertyType.SIMPLE, "==", "key1");
    command1 = new TestCommand(CommandType.VERIFY, new TestTrigger(1000, 500, "simpleValue1-new"), arg1);
    commandList.add(command1);

    arg1 = new ZnodeOpArg(pathChild1, ZnodePropertyType.ZNODE, "==");
    command1 = new TestCommand(CommandType.VERIFY, new TestTrigger(1000, 500, recordNew), arg1);
    commandList.add(command1);

    Map<TestCommand, Boolean> results = TestExecutor.executeTest(commandList, ZK_ADDR);
    for (Map.Entry<TestCommand, Boolean> entry : results.entrySet())
    {
      Assert.assertFalse(entry.getValue());
    }

    logger.info("END: " + new Date(System.currentTimeMillis()));
  }


  @Test ()
  public void testDataTriggerWithTimeout() throws Exception
  {
    logger.info("RUN: " + new Date(System.currentTimeMillis()));
    List<TestCommand> commandList = new ArrayList<TestCommand>();

    // test case for data trigger with timeout
    final String pathChild1 = PREFIX + "/dataTriggerWithTimeout_child1";

    final ZNRecord record = getExampleZNRecord();
    ZNRecord recordNew = new ZNRecord(record);
    recordNew.setSimpleField("ideal_state_mode", IdealStateConfigProperty.AUTO.toString());
    ZnodeOpArg arg1 = new ZnodeOpArg(pathChild1, ZnodePropertyType.ZNODE, "+", recordNew);
    TestCommand command1 = new TestCommand(CommandType.MODIFY, new TestTrigger(0, 8000, record), arg1);
    commandList.add(command1);

    arg1 = new ZnodeOpArg(pathChild1, ZnodePropertyType.ZNODE, "==");
    command1 = new TestCommand(CommandType.VERIFY, new TestTrigger(9000, 500, recordNew), arg1);
    commandList.add(command1);

    // start a separate thread to change znode at pathChild1
    new Thread()
    {
      @Override
      public void run()
      {
        try
        {
          Thread.sleep(3000);
          final ZkClient zkClient = new ZkClient(ZK_ADDR);
          zkClient.setZkSerializer(new ZNRecordSerializer());
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

    Map<TestCommand, Boolean> results = TestExecutor.executeTest(commandList, ZK_ADDR);
    for (Map.Entry<TestCommand, Boolean> entry : results.entrySet())
    {
      Assert.assertTrue(entry.getValue());
      // System.out.println(entry.getValue() + ":" + entry.getKey());
    }

  }

  ZkClient _zkClient;

  @BeforeClass (groups = {"unitTest"})
  public void beforeClass()
  {
    _zkClient = new ZkClient(ZK_ADDR);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    if (_zkClient.exists(PREFIX))
    {
      _zkClient.deleteRecursive(PREFIX);
    }

  }


  @AfterClass
  public void afterClass()
  {
  	_zkClient.close();
  }


  private ZNRecord getExampleZNRecord()
  {
    ZNRecord record = new ZNRecord("TestDB");
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

}

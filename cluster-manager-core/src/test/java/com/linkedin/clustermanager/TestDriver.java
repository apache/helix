package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.testng.Assert;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.TestHelper.DummyProcessResult;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;
import com.linkedin.clustermanager.tools.TestCommand;
import com.linkedin.clustermanager.tools.TestCommand.CommandType;
import com.linkedin.clustermanager.tools.TestExecutor;
import com.linkedin.clustermanager.tools.TestExecutor.ZnodePropertyType;
import com.linkedin.clustermanager.tools.TestTrigger;
import com.linkedin.clustermanager.tools.ZnodeOpArg;

public class TestDriver
{
  private static Logger LOG = Logger.getLogger(TestDriver.class);
  private static final String ZK_ADDR            = "localhost:2183";
  private static final String ZK_LOG_DIR         = "/tmp/logs";
  private static final String ZK_DATA_DIR        = "/tmp/dataDir";
  private static final AtomicReference<ZkClient> _zkClient  = new AtomicReference<ZkClient>(null);

  private static final String CLUSTER_PREFIX     = "ESPRESSO_STORAGE_TestDriver";
  private static final String STATE_MODEL        = "MasterSlave";
  private static final String TEST_DB_PREFIX     = "TestDB";
  private static final int    START_PORT         = 12918;
  private static final String CONTROLLER_PREFIX  = "controller";
  private static final String PARTICIPANT_PREFIX = "localhost";
  private static final Random RANDOM             = new Random();
  private static final PropertyJsonSerializer<ZNRecord> SERIALIZER = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);

  private static final Map<String, TestInfo> _testInfoMap = new ConcurrentHashMap<String, TestInfo>();

  private static class TestInfo
  {
    public final String _clusterName;
    public final int _numDb;
    public final int _numPartitionsPerDb;
    public final int _numNode;
    public final int _replica;
    
    public final Map<String, Thread> _threadMap = new ConcurrentHashMap<String, Thread>();
    public final CountDownLatch _instanceStarted;
    
    public TestInfo(String clusterName, int numDb, int numPartitionsPerDb, int numNode, int replica)
    {
      _clusterName = clusterName;
      _numDb = numDb;
      _numPartitionsPerDb = numPartitionsPerDb;
      _numNode = numNode;
      _replica = replica;
      _instanceStarted = new CountDownLatch(_numNode);
    }
  }
  
  /*
  public static ZkServer startZk()
  {
    ZKClientPool.reset();

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
      }
    };

    int port = Integer.parseInt(ZK_ADDR.substring(ZK_ADDR.lastIndexOf(':') + 1));
    ZkServer zkServer = new ZkServer(ZK_DATA_DIR, ZK_LOG_DIR, defaultNameSpace, port);
    zkServer.start();
    
    if (_zkClient.compareAndSet(null, new ZkClient(ZK_ADDR)) == true)
    {
      _zkClient.get().setZkSerializer(new ZNRecordSerializer());
    }
    else
    {
      LOG.warn("zkClient has been initialized before zk starts");
    }
    
    return zkServer;
  }

  public static void stopZk(ZkServer zkServer)
  {
    if (zkServer != null)
    {
      zkServer.shutdown();
    }
    _zkClient.set(null);
  }
  */
  
  public static void setupClusterWithoutRebalance(String uniqTestName,
                                  int numDb,
                                  int numPartitionPerDb,
                                  int numNodes,
                                  int replica) throws Exception
  {
    setupCluster(uniqTestName, numDb, numPartitionPerDb, numNodes, replica, false);
  }
  
  public static void setupCluster(String uniqTestName,
                                  int numDb,
                                  int numPartitionPerDb,
                                  int numNodes,
                                  int replica) throws Exception
  {
    setupCluster(uniqTestName, numDb, numPartitionPerDb, numNodes, replica, true);
  }
  
  public static void setupCluster(String uniqTestName,
                                  int numDb,
                                  int numPartitionPerDb,
                                  int numNodes,
                                  int replica, boolean doRebalance) throws Exception
  {
    if (_zkClient.compareAndSet(null, new ZkClient(ZK_ADDR)) == true)
    {
      LOG.info("zkClient is null, zk server may be started externally");
      _zkClient.get().setZkSerializer(new ZNRecordSerializer());
    }

    String clusterName = CLUSTER_PREFIX + "_" + uniqTestName;
    if (_zkClient.get().exists("/" + clusterName))
    {
      LOG.warn("test cluster already exists:" + clusterName + ", test name:" + uniqTestName
          + " is not unique or test has been run without cleaning up zk; deleting it");
      _zkClient.get().deleteRecursive("/" + clusterName);
    }

    if (_testInfoMap.containsKey(uniqTestName))
    {
      LOG.warn("test info already exists:" + uniqTestName
          + " is not unique or test has been run without cleaning up test info map; removing it");
      _testInfoMap.remove(uniqTestName);
    }
    else
    {
      _testInfoMap.put(uniqTestName, new TestInfo(clusterName, numDb, numPartitionPerDb
                                                  , numNodes, replica));
    }

    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(clusterName, true);

    for (int i = 0; i < numNodes; i++)
    {
      int port = START_PORT + i;
      setupTool.addInstanceToCluster(clusterName, PARTICIPANT_PREFIX + ":" + port);
    }

    for (int i = 0; i < numDb; i++)
    {
      String dbName = TEST_DB_PREFIX + i;
      setupTool.addResourceGroupToCluster(clusterName, dbName, numPartitionPerDb, STATE_MODEL);
      if (doRebalance)
      {
        setupTool.rebalanceStorageCluster(clusterName, dbName, replica);
      }
    }
  }

  public static void startDummyParticipants(String uniqTestName, int numNodes) throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test info does NOT exists:" + uniqTestName;
      throw new Exception(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    String clusterName = testInfo._clusterName;
    
    for (int i = 0; i < numNodes; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);

      DummyProcessResult result = TestHelper.startDummyProcess(ZK_ADDR, clusterName, instanceName, null);
      testInfo._threadMap.put(instanceName, result._thread);
      testInfo._instanceStarted.countDown();
    }
  }

  public static void startController(String uniqTestName) throws Exception
  {
    startController(uniqTestName, 1);
  }

  public static void startController(String uniqTestName, int numController) throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test info does NOT exists:" + uniqTestName;
      throw new Exception(errMsg);
    }
    
    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    String clusterName = testInfo._clusterName;

    for (int i = 0; i < numController; i++)
    {
      String controllerName = CONTROLLER_PREFIX + "_" + i;
      DummyProcessResult result = TestHelper.startClusterController(clusterName, controllerName,
                                            ZK_ADDR, ClusterManagerMain.STANDALONE, null);
      testInfo._threadMap.put(controllerName, result._thread);
    }
  }

  public static void verifyCluster(String uniqTestName) throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test info does NOT exists:" + uniqTestName;
      throw new Exception(errMsg);
    }
    
    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    String clusterName = testInfo._clusterName;
    
    // verify external view
    String liveInstancePath = "/" + clusterName + "/" + ClusterPropertyType.LIVEINSTANCES.toString();
    List<String> liveInstances = _zkClient.get().getChildren(liveInstancePath);
    String configInstancePath = "/" + clusterName + "/" + ClusterPropertyType.CONFIGS.toString();
    List<String> instances = _zkClient.get().getChildren(configInstancePath);
    instances.removeAll(liveInstances);
    
    List<TestCommand> commandList = new ArrayList<TestCommand>();

    for (int i = 0; i < testInfo._numDb; i++)
    {
      String idealStatePath = "/" + clusterName + "/" + ClusterPropertyType.IDEALSTATES.toString() 
          + "/" + TEST_DB_PREFIX + i;
      ZNRecord idealState = _zkClient.get().<ZNRecord>readData(idealStatePath);
      ZNRecord externalView = calculateExternalViewFromIdealState(idealState, instances);
    
      String externalViewPath = "/" + clusterName + "/" + ClusterPropertyType.EXTERNALVIEW.toString() 
          + "/" + TEST_DB_PREFIX + i;
      
      ZnodeOpArg arg = new ZnodeOpArg(externalViewPath, ZnodePropertyType.ZNODE, "=="); 
      TestCommand command = new TestCommand(CommandType.VERIFY, 
                                         new TestTrigger(1000, 60 * 1000, externalView), arg);
      commandList.add(command);
    }
 
    Map<String, Boolean> results = TestExecutor.executeTest(commandList, ZK_ADDR);
    for (Map.Entry<String, Boolean> entry : results.entrySet())
    {
      // System.err.println(entry.getValue() + ":" + entry.getKey());
      LOG.info(entry.getValue() + ":" + entry.getKey());
      // System.err.println(entry.getValue() + ":" + uniqTestName + "@" + new Date().getTime());
      Assert.assertTrue(entry.getValue());
    }
    
    // LOG.info("verify cluster:" + clusterName + ", result:" + result);
    // System.err.println("verify cluster:" + clusterName + ", result:" + result);
    // TODO verify other states
  }

  public static void stopCluster(String uniqTestName) throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test info does NOT exists:" + uniqTestName;
      throw new Exception(errMsg);
    }
    TestInfo testInfo = _testInfoMap.remove(uniqTestName);

    // stop controller first
    for (Iterator<Entry<String, Thread>> it = testInfo._threadMap.entrySet().iterator(); it.hasNext();)
    {
      Map.Entry<String, Thread> entry = it.next();
      String instanceName = entry.getKey();
      if (instanceName.startsWith(CONTROLLER_PREFIX))
      {
        it.remove();
        Thread thread = entry.getValue();
        thread.interrupt();
      }
    }

    Thread.sleep(1000);
    
    // stop the rest
    for (Map.Entry<String, Thread> entry : testInfo._threadMap.entrySet())
    {
      Thread thread = entry.getValue();
      thread.interrupt();
    }
  }
  
  public static void randomFailWithCustomIdealState(String uniqTestName, int numFail, int numRecover, 
                                            int percentage, long timeForEachStep)
  throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test info does NOT exists:" + uniqTestName;
      throw new Exception(errMsg);
    }
    
    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    testInfo._instanceStarted.await();
    
    String clusterName = testInfo._clusterName;
    
    if (testInfo._numDb > 1)
    {
      String errMsg = "TestDriver.randomFailWithCustomIs() currently doesn't support numDb > 1:" 
              + uniqTestName;
      throw new UnsupportedOperationException(errMsg);
    }
    
    List<String> instanceNames = new ArrayList<String>();

    for (int i = 0; i < testInfo._numNode; i++)
    {
      int port = START_PORT + i;
      instanceNames.add(PARTICIPANT_PREFIX + "_" + port);
    }

    // do the test
    TestCommand command;
    ZnodeOpArg arg;
    List<TestCommand> commandList = new ArrayList<TestCommand>();
    
    // IS change commands
    for (int i = 0; i < testInfo._numDb; i++)
    {
      String dbName = TEST_DB_PREFIX + i;
      ZNRecord destIS = IdealStateCalculatorForStorageNode.calculateIdealState(instanceNames, 
               testInfo._numPartitionsPerDb, testInfo._replica - 1, dbName, "MASTER", "SLAVE");
      // destIS.setId(dbName);
      destIS.setSimpleField("ideal_state_mode", IdealStateConfigProperty.CUSTOMIZED.toString());
      destIS.setSimpleField("partitions", Integer.toString(testInfo._numPartitionsPerDb));
      destIS.setSimpleField("state_model_def_ref", STATE_MODEL);
      String idealStatePath = "/" + clusterName + "/" + ClusterPropertyType.IDEALSTATES.toString() 
          + "/" + TEST_DB_PREFIX + i;
      ZNRecord initIS = _zkClient.get().<ZNRecord> readData(idealStatePath);
      int totalStep = calcuateNumTransitions(initIS, destIS);
      LOG.info("initIS:" + initIS);
      LOG.info("destIS:" + destIS);
      LOG.info("totalSteps:" + totalStep);
      
      ZNRecord nextIS = initIS;
      int steps = totalStep * percentage / 100;
      long nextTriggerTime = 1000;
      while (totalStep > steps)
      {
        nextIS = nextIdealState(nextIS, destIS, steps);
        arg = new ZnodeOpArg(idealStatePath, ZnodePropertyType.ZNODE, "+", nextIS);
        command = new TestCommand(CommandType.MODIFY, new TestTrigger(nextTriggerTime), arg);
        commandList.add(command);
        totalStep -= steps;
        nextTriggerTime += timeForEachStep;
      }
      
      arg = new ZnodeOpArg(idealStatePath, ZnodePropertyType.ZNODE, "+", destIS);
      command = new TestCommand(CommandType.MODIFY, new TestTrigger(nextTriggerTime), arg);
      commandList.add(command);
    }

    // fail commands
    if (numFail > 1)
    {
      String errMsg = "TestDriver.randomFailWithCustomIs() currently doesn't support numFail > 1:" 
              + uniqTestName;
      throw new UnsupportedOperationException(errMsg);
    }
    for (int i = 0; i < numFail; i++)
    {
      String failHost = PARTICIPANT_PREFIX + "_" + (START_PORT + i); 
      Thread thread = testInfo._threadMap.get(failHost);
      if (thread == null)
      {
        String errMsg = "TestDriver.randomFailWithCustomIs() fail thread is null, failHost:" + failHost;
        throw new Exception(errMsg);
      }
      command = new TestCommand(CommandType.STOP, new TestTrigger(timeForEachStep + 2000), thread);
      commandList.add(command);
    }
    
    if (numRecover > 0)
    {
      String errMsg = "TestDriver.randomFailWithCustomIs() currently doesn't support numRecover > 0:" 
              + uniqTestName;
      throw new UnsupportedOperationException(errMsg);
    }
    
    // Map<String, Boolean> results = 
    TestExecutor.executeTest(commandList, ZK_ADDR);
    
  }
  
  private static ZNRecord calculateExternalViewFromIdealState(ZNRecord idealState, List<String> failInstances)
  {
    ZNRecord externalView = new ZNRecord(idealState.getId());

    // externalView.setId();
    
    for (Map.Entry<String, Map<String, String>> mapEntry : idealState.getMapFields().entrySet())
    {
      
      for (String failInstance : failInstances)
      {
        mapEntry.getValue().remove(failInstance);
      }
      externalView.setMapField(mapEntry.getKey(), mapEntry.getValue());
    }

    return externalView;
  }
  
  private static List<String[]> findAllUnfinishPairs(ZNRecord cur, ZNRecord dest)
  {
    // find all (host, resource) pairs that haven't reached destination state
    List<String[]> list = new ArrayList<String[]>();
    Map<String, Map<String, String>> map = dest.getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : map.entrySet())
    {
      String resourceKey = entry.getKey();
      Map<String, String> hostMap = entry.getValue();
      for (Map.Entry<String, String> hostEntry : hostMap.entrySet())
      {
        String host = hostEntry.getKey();
        String destState = hostEntry.getValue();
        Map<String, String> curHostMap = cur.getMapField(resourceKey);

        String curState = null;
        if (curHostMap != null)
        {
          curState = curHostMap.get(host);
        }

        String[] pair = new String[3];
        if (curState == null)
        {
          if (destState.equalsIgnoreCase("SLAVE")) 
          {
            pair[0] = new String(resourceKey);
            pair[1] = new String(host);
            pair[2] = new String("1");    // number of transitions required
            list.add(pair);
          }
          else if (destState.equalsIgnoreCase("MASTER"))
          {
            pair[0] = new String(resourceKey);
            pair[1] = new String(host);
            pair[2] = new String("2");    // number of transitions required
            list.add(pair);
          }
        }
        else 
        {
          if (curState.equalsIgnoreCase("SLAVE") && destState.equalsIgnoreCase("MASTER"))
          {
            pair[0] = new String(resourceKey);
            pair[1] = new String(host);
            pair[2] = new String("1");    // number of transitions required
            list.add(pair);
          }
        }
      }
    }
    return list;
  }
  
  private static int calcuateNumTransitions(ZNRecord start, ZNRecord end)
  {
    int totalSteps = 0;
    List<String[]> list = findAllUnfinishPairs(start, end);
    for (String[] pair : list)
    {
      totalSteps += Integer.parseInt(pair[2]);
    }
    return totalSteps;
  }
  
  private static ZNRecord nextIdealState(final ZNRecord cur, final ZNRecord dest, final int steps) 
      throws PropertyStoreException
  {
    // get a deep copy
    ZNRecord next = SERIALIZER.deserialize(SERIALIZER.serialize(cur));
    List<String[]> list = findAllUnfinishPairs(cur, dest);

    // randomly pick up pairs that haven't reached destination state and progress
    for (int i = 0; i < steps; i++)
    {
      int randomInt = RANDOM.nextInt(list.size());
      String[] pair = list.get(randomInt);
      String curState = null;
      Map<String, String> curHostMap = next.getMapField(pair[0]);
      if (curHostMap != null)
      {
        curState = curHostMap.get(pair[1]);
      }
      final String destState = dest.getMapField(pair[0]).get(pair[1]);

      // TODO generalize it using state-model
      if (curState == null && destState != null)
      {
        Map<String, String> hostMap = next.getMapField(pair[0]);
        if (hostMap == null)
        {
          hostMap = new HashMap<String, String>();
        }
        hostMap.put(pair[1], "SLAVE");
        next.setMapField(pair[0], hostMap);
      }
      else if (curState.equalsIgnoreCase("SLAVE") && destState != null
          && destState.equalsIgnoreCase("MASTER"))
      {
        next.getMapField(pair[0]).put(pair[1], "MASTER");
      }
      else
      {
        LOG.error("fail to calculate the next ideal state");
      }
      curState = next.getMapField(pair[0]).get(pair[1]);
      if (curState != null && curState.equalsIgnoreCase(destState))
      {
        list.remove(randomInt);
      }
    }

    LOG.info("nextIS:" + next);
    return next;
  }
}

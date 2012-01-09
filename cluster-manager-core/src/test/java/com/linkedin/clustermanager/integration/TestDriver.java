package com.linkedin.clustermanager.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.StartCMResult;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.model.IdealState.IdealStateModeProperty;
import com.linkedin.clustermanager.model.IdealState.IdealStateProperty;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;
import com.linkedin.clustermanager.tools.TestCommand;
import com.linkedin.clustermanager.tools.TestCommand.CommandType;
import com.linkedin.clustermanager.tools.TestCommand.NodeOpArg;
import com.linkedin.clustermanager.tools.TestExecutor;
import com.linkedin.clustermanager.tools.TestExecutor.ZnodePropertyType;
import com.linkedin.clustermanager.tools.TestTrigger;
import com.linkedin.clustermanager.tools.ZnodeOpArg;

public class TestDriver
{
  private static Logger LOG = Logger.getLogger(TestDriver.class);
  private static final String ZK_ADDR = ZkIntegrationTestBase.ZK_ADDR; // "localhost:2183";
  // private static final int DEFAULT_SESSION_TIMEOUT = 30000;
  // private static final int DEFAULT_CONNECTION_TIMEOUT = Integer.MAX_VALUE;
  // private static final ZkClient _zkClient = new ZkClient(ZK_ADDR, DEFAULT_SESSION_TIMEOUT,
  //    DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());

  private static final String CLUSTER_PREFIX = "TestDriver";
  private static final String STATE_MODEL = "MasterSlave";
  private static final String TEST_DB_PREFIX = "TestDB";
  private static final int START_PORT = 12918;
  private static final String CONTROLLER_PREFIX = "controller";
  private static final String PARTICIPANT_PREFIX = "localhost";
  private static final Random RANDOM = new Random();
  private static final PropertyJsonSerializer<ZNRecord> SERIALIZER = new PropertyJsonSerializer<ZNRecord>(
      ZNRecord.class);

  private static final Map<String, TestInfo> _testInfoMap = new ConcurrentHashMap<String, TestInfo>();

  public static class TestInfo
  {
  	public final ZkClient _zkClient;
    public final String _clusterName;
    public final int _numDb;
    public final int _numPartitionsPerDb;
    public final int _numNode;
    public final int _replica;

    public final Map<String, ZNRecord> _idealStateMap = new ConcurrentHashMap<String, ZNRecord>();
    public final Map<String, StartCMResult> _startCMResultMap = new ConcurrentHashMap<String, StartCMResult>();

    public TestInfo(String clusterName, ZkClient zkClient, int numDb, int numPartitionsPerDb, int numNode, int replica)
    {
			this._clusterName = clusterName;
			this._zkClient = zkClient;
      this._numDb = numDb;
      this._numPartitionsPerDb = numPartitionsPerDb;
      this._numNode = numNode;
      this._replica = replica;
    }
  }

  public static TestInfo getTestInfo(String uniqTestName)
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "Cluster hasn't been setup for " + uniqTestName;
      throw new IllegalArgumentException(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    return testInfo;
  }

  public static void setupClusterWithoutRebalance(String uniqTestName, ZkClient zkClient,
  		int numDb, int numPartitionPerDb, int numNodes, int replica) throws Exception
  {
    setupCluster(uniqTestName, zkClient, numDb, numPartitionPerDb, numNodes, replica, false);
  }

  public static void setupCluster(String uniqTestName, ZkClient zkClient, int numDb,
  		int numPartitionPerDb, int numNodes, int replica) throws Exception
  {
    setupCluster(uniqTestName, zkClient, numDb, numPartitionPerDb, numNodes, replica, true);
  }

  public static void setupCluster(String uniqTestName, ZkClient zkClient, int numDb,
  		int numPartitionPerDb, int numNodes, int replica, boolean doRebalance) throws Exception
  {

    String clusterName = CLUSTER_PREFIX + "_" + uniqTestName;
    if (zkClient.exists("/" + clusterName))
    {
      LOG.warn("test cluster already exists:" + clusterName + ", test name:" + uniqTestName
          + " is not unique or test has been run without cleaning up zk; deleting it");
      zkClient.deleteRecursive("/" + clusterName);
    }

    if (_testInfoMap.containsKey(uniqTestName))
    {
      LOG.warn("test info already exists:" + uniqTestName
          + " is not unique or test has been run without cleaning up test info map; removing it");
      _testInfoMap.remove(uniqTestName);
    }
    TestInfo testInfo = new TestInfo(clusterName, zkClient, numDb, numPartitionPerDb,
    		numNodes, replica);
    _testInfoMap.put(uniqTestName, testInfo);


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

        String idealStatePath = "/" + clusterName + "/" + PropertyType.IDEALSTATES.toString()
            + "/" + dbName;
        ZNRecord idealState = zkClient.<ZNRecord> readData(idealStatePath);
        testInfo._idealStateMap.put(dbName, idealState);
      }
    }
  }

  /**
   * starting a dummy participant with a given id
   *
   * @param uniqueTestName
   * @param nodeId
   */
  public static void startDummyParticipant(String uniqTestName, int nodeId) throws Exception
  {
    startDummyParticipants(uniqTestName, new int[] { nodeId });
  }

  public static void startDummyParticipants(String uniqTestName, int[] nodeIds) throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test cluster hasn't been setup:" + uniqTestName;
      throw new Exception(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    String clusterName = testInfo._clusterName;

    for (int id : nodeIds)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + id);

      if (testInfo._startCMResultMap.containsKey(instanceName))
      {
        LOG.warn("Dummy participant:" + instanceName + " has already started; skip starting it");
      } else
      {
        StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, clusterName, instanceName,
            null);
        testInfo._startCMResultMap.put(instanceName, result);
        // testInfo._instanceStarted.countDown();
      }
    }
  }

  public static void startController(String uniqTestName) throws Exception
  {
    startController(uniqTestName, new int[] { 0 });
  }

  public static void startController(String uniqTestName, int[] nodeIds) throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test cluster hasn't been setup:" + uniqTestName;
      throw new Exception(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    String clusterName = testInfo._clusterName;

    for (int id : nodeIds)
    {
      String controllerName = CONTROLLER_PREFIX + "_" + id;
      if (testInfo._startCMResultMap.containsKey(controllerName))
      {
        LOG.warn("Controller:" + controllerName + " has already started; skip starting it");
      } else
      {
        StartCMResult result = TestHelper.startClusterController(clusterName, controllerName,
            ZK_ADDR, ClusterManagerMain.STANDALONE, null);
        testInfo._startCMResultMap.put(controllerName, result);
      }
    }
  }

  public static void verifyCluster(String uniqTestName) throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test cluster hasn't been setup:" + uniqTestName;
      throw new Exception(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    String clusterName = testInfo._clusterName;
    ZkClient zkClient = testInfo._zkClient;

    // verify external view
    String liveInstancePath = "/" + clusterName + "/"
        + PropertyType.LIVEINSTANCES.toString();
    List<String> liveInstances = zkClient.getChildren(liveInstancePath);
    String configInstancePath = "/" + clusterName + "/" + PropertyType.CONFIGS.toString();
    List<String> failInstances = zkClient.getChildren(configInstancePath);
    failInstances.removeAll(liveInstances);

    List<TestCommand> commandList = new ArrayList<TestCommand>();

    for (int i = 0; i < testInfo._numDb; i++)
    {
      // String idealStatePath = "/" + clusterName + "/" + PropertyType.IDEALSTATES.toString()
      //    + "/" + TEST_DB_PREFIX + i;
      // ZNRecord idealState = _zkClient.<ZNRecord> readData(idealStatePath);
      String dbName = TEST_DB_PREFIX + i;
      ZNRecord idealState = testInfo._idealStateMap.get(dbName);

      ZNRecord externalView = calculateExternalViewFromIdealState(idealState, failInstances);

      String externalViewPath = "/" + clusterName + "/"
          + PropertyType.EXTERNALVIEW.toString() + "/" + TEST_DB_PREFIX + i;

      ZnodeOpArg arg = new ZnodeOpArg(externalViewPath, ZnodePropertyType.ZNODE, "==");
      TestCommand command = new TestCommand(CommandType.VERIFY, new TestTrigger(1000, 120 * 1000,
          externalView), arg);
      commandList.add(command);
    }

    Map<TestCommand, Boolean> results = TestExecutor.executeTest(commandList, ZK_ADDR);
    for (Map.Entry<TestCommand, Boolean> entry : results.entrySet())
    {
      System.out.println(entry.getValue() + ":" + entry.getKey());
      // LOG.info(entry.getValue() + ":" + entry.getKey());
      AssertJUnit.assertTrue(entry.getValue());
    }

    // LOG.info("verify cluster:" + clusterName + ", result:" + result);
    // System.err.println("verify cluster:" + clusterName + ", result:" +
    // result);
    // TODO verify other states
  }

  public static void stopCluster(String uniqTestName) throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test cluster hasn't been setup:" + uniqTestName;
      throw new Exception(errMsg);
    }
    TestInfo testInfo = _testInfoMap.remove(uniqTestName);

    // stop controller first
    for (Iterator<Entry<String, StartCMResult>> it = testInfo._startCMResultMap.entrySet()
        .iterator(); it.hasNext();)
    {
      Map.Entry<String, StartCMResult> entry = it.next();
      String instanceName = entry.getKey();
      if (instanceName.startsWith(CONTROLLER_PREFIX))
      {
        it.remove();
        ClusterManager manager = entry.getValue()._manager;
        manager.disconnect();
        Thread thread = entry.getValue()._thread;
        thread.interrupt();
      }
    }

    Thread.sleep(1000);

    // stop the rest
    for (Map.Entry<String, StartCMResult> entry : testInfo._startCMResultMap.entrySet())
    {
      ClusterManager manager = entry.getValue()._manager;
      manager.disconnect();
      Thread thread = entry.getValue()._thread;
      thread.interrupt();
    }
  }

  public static void stopDummyParticipant(String uniqTestName, long at, int nodeId)
      throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test cluster hasn't been setup:" + uniqTestName;
      throw new Exception(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    // String clusterName = testInfo._clusterName;

    String failHost = PARTICIPANT_PREFIX + "_" + (START_PORT + nodeId);
    StartCMResult result = testInfo._startCMResultMap.remove(failHost);

    // TODO need sync
    if (result == null || result._manager == null || result._thread == null)
    {
      String errMsg = "Dummy participant:" + failHost + " seems not running";
      LOG.error(errMsg);
    } else
    {
      // System.err.println("try to stop participant: " + result._manager.getInstanceName());
      NodeOpArg arg = new NodeOpArg(result._manager, result._thread);
      TestCommand command = new TestCommand(CommandType.STOP, new TestTrigger(at), arg);
      List<TestCommand> commandList = new ArrayList<TestCommand>();
      commandList.add(command);
      TestExecutor.executeTestAsync(commandList, ZK_ADDR);
    }
  }


  public static void setIdealState(String uniqTestName, long at, int percentage)
  throws Exception
  {
    if (!_testInfoMap.containsKey(uniqTestName))
    {
      String errMsg = "test cluster hasn't been setup:" + uniqTestName;
      throw new Exception(errMsg);
    }
    TestInfo testInfo = _testInfoMap.get(uniqTestName);
    String clusterName = testInfo._clusterName;
    List<String> instanceNames = new ArrayList<String>();

    for (int i = 0; i < testInfo._numNode; i++)
    {
      int port = START_PORT + i;
      instanceNames.add(PARTICIPANT_PREFIX + "_" + port);
    }

    List<TestCommand> commandList = new ArrayList<TestCommand>();
    for (int i = 0; i < testInfo._numDb; i++)
    {
      String dbName = TEST_DB_PREFIX + i;
      ZNRecord destIS = IdealStateCalculatorForStorageNode.calculateIdealState(instanceNames,
              testInfo._numPartitionsPerDb, testInfo._replica - 1, dbName, "MASTER","SLAVE");
      // destIS.setId(dbName);
      destIS.setSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString(), IdealStateModeProperty.CUSTOMIZED.toString());
      destIS.setSimpleField(IdealStateProperty.RESOURCES.toString(), Integer.toString(testInfo._numPartitionsPerDb));
      destIS.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), STATE_MODEL);
      String idealStatePath = "/" + clusterName
                            + "/" + PropertyType.IDEALSTATES.toString()
                            + "/" + TEST_DB_PREFIX + i;
      ZNRecord initIS = new ZNRecord(dbName); // _zkClient.<ZNRecord> readData(idealStatePath);
      initIS.setSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString(), IdealStateModeProperty.CUSTOMIZED.toString());
      initIS.setSimpleField(IdealStateProperty.RESOURCES.toString(), Integer.toString(testInfo._numPartitionsPerDb));
      initIS.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), "MasterSlave");
      int totalStep = calcuateNumTransitions(initIS, destIS);
      // LOG.info("initIS:" + initIS);
      // LOG.info("destIS:" + destIS);
      // LOG.info("totalSteps from initIS to destIS:" + totalStep);
      // System.out.println("initIS:" + initIS);
      // System.out.println("destIS:" + destIS);
      System.out.println("totalSteps from initIS to destIS:" + totalStep);


      ZNRecord nextIS;
      int step = totalStep * percentage / 100;
      nextIS = nextIdealState(initIS, destIS, step);
      testInfo._idealStateMap.put(dbName, nextIS);
      ZnodeOpArg arg = new ZnodeOpArg(idealStatePath, ZnodePropertyType.ZNODE, "+", nextIS);
      TestCommand command = new TestCommand(CommandType.MODIFY, new TestTrigger(at), arg);
      commandList.add(command);
    }

    TestExecutor.executeTestAsync(commandList, ZK_ADDR);

  }

  private static ZNRecord calculateExternalViewFromIdealState(ZNRecord idealState,
      List<String> failInstances)
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
            pair[2] = new String("1"); // number of transitions required
            list.add(pair);
          } else if (destState.equalsIgnoreCase("MASTER"))
          {
            pair[0] = new String(resourceKey);
            pair[1] = new String(host);
            pair[2] = new String("2"); // number of transitions required
            list.add(pair);
          }
        } else
        {
          if (curState.equalsIgnoreCase("SLAVE") && destState.equalsIgnoreCase("MASTER"))
          {
            pair[0] = new String(resourceKey);
            pair[1] = new String(host);
            pair[2] = new String("1"); // number of transitions required
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

    // randomly pick up pairs that haven't reached destination state and
    // progress
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
      } else if (curState.equalsIgnoreCase("SLAVE") && destState != null
          && destState.equalsIgnoreCase("MASTER"))
      {
        next.getMapField(pair[0]).put(pair[1], "MASTER");
      } else
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

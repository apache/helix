package com.linkedin.clustermanager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyStateModel;
import com.linkedin.clustermanager.mock.storage.DummyProcess.DummyStateModelFactory;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;
import com.linkedin.clustermanager.tools.ZnodeModCommand;
import com.linkedin.clustermanager.tools.ZnodeModDesc;
import com.linkedin.clustermanager.tools.ZnodeModDesc.ZnodePropertyType;
import com.linkedin.clustermanager.tools.ZnodeModExecutor;
import com.linkedin.clustermanager.tools.ZnodeModVerifier;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestFrameworkExample
{
  private static Logger logger = Logger.getLogger(TestFrameworkExample.class);
  protected static final String ZK_ADDR       = "localhost:2183";
  protected final String CLASS_NAME           = getShortClassName();
  protected final String CLUSTER_NAME         = "ESPRESSO_STORAGE_" + CLASS_NAME;
  protected static final int NODE_NR          = 10;
  protected static final int START_PORT       = 12918;
  protected static final String STATE_MODEL   = "MasterSlave";
  private static final String TEST_DB         = "TestDB";
  protected ClusterSetup _setupTool           = null;
  private static final int  REPLICA           = 3;
  private static final int  PARTITIONS        = 128;
  private final Random RANDOM_GEN             = new Random();
  private final ScheduledExecutorService _executor = Executors.newScheduledThreadPool(1);
  private final PropertyJsonSerializer<ZNRecord> _serializer = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
  private final Map<String, Tuple<Thread, ClusterManager>> 
        _threadMap = new ConcurrentHashMap<String, Tuple<Thread, ClusterManager>>();
  private final String _idealStatePath        = "/" + CLUSTER_NAME + "/IDEALSTATES/" + TEST_DB;

  public class Tuple<F, S>
  {
    private F _first;
    private S _second;

    public Tuple(F f, S s)
    {
      this._first = f;
      this._second = s;
    }

    public F getFirst()
    {
      return _first;
    }

    public S getSecond()
    {
      return _second;
    }

    public void setFirst(F f)
    {
      this._first = f;
    }

    public void setSecond(S s)
    {
      this._second = s;
    }
  }

  @Test
  public void testFrameworkExample() throws Exception
  {
    logger.info("START at " + new Date(System.currentTimeMillis()));

    ZkServer zkServer = TestHelper.startZkSever(ZK_ADDR, "/" + CLUSTER_NAME);
    List<String> instanceNames = new ArrayList<String>();
    _setupTool = new ClusterSetup(ZK_ADDR);

    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME,
                                         TEST_DB,
                                         PARTITIONS,
                                         STATE_MODEL,
                                         IdealStateConfigProperty.CUSTOMIZED.toString());
    for (int i = 0; i < NODE_NR; i++)
    {
      int port = START_PORT + i;
      instanceNames.add("localhost_" + port);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, "localhost:" + port);
    }

    final ZkClient zkClient = ZKClientPool.getZkClient(ZK_ADDR);
    final ZNRecord destIS = IdealStateCalculatorForStorageNode
        .calculateIdealState(instanceNames, 
                             PARTITIONS,
                             REPLICA - 1,
                             TEST_DB,
                             "MASTER",
                             "SLAVE");
    destIS.setId(TEST_DB);
    destIS.setSimpleField("ideal_state_mode", IdealStateConfigProperty.CUSTOMIZED.toString());
    destIS.setSimpleField("partitions", Integer.toString(PARTITIONS));
    destIS.setSimpleField("state_model_def_ref", STATE_MODEL);
    ZNRecord initIS = zkClient.<ZNRecord> readData(_idealStatePath);
    System.out.println("initIS:" + initIS);
    System.out.println("destIS:" + destIS);

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (START_PORT + i);
      if (_threadMap.get(instanceName) != null)
      {
        logger.error("fail to start participant:" + instanceName +
                     " because there is already a thread with same instanceName running");
      }
      else
      {
        startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName, null);
      }
    }

    // start controller
    String controllerName = "controller_0";
    startClusterController(CLUSTER_NAME, controllerName, ZK_ADDR, ClusterManagerMain.STANDALONE, null);

    // do test
    ZnodeModDesc testDesc = new ZnodeModDesc("TestFrameworkExample");
    ZnodeModCommand command;
    
    ZNRecord nextIS = nextIdealState(initIS, destIS, 10);
    command = new ZnodeModCommand(1000, 1000, _idealStatePath, ZnodePropertyType.ZNODE, "+", nextIS);
    testDesc.addCommand(command);
    
    nextIS = nextIdealState(nextIS, destIS, 20);
    command = new ZnodeModCommand(10000, 1000, _idealStatePath, ZnodePropertyType.ZNODE, "+", nextIS);
    testDesc.addCommand(command);
    
    command = new ZnodeModCommand(20000, 1000, _idealStatePath, ZnodePropertyType.ZNODE, "+", destIS);
    testDesc.addCommand(command);

    ZNRecord destEV = getExternalViewFromIdealState(destIS);
    ZnodeModVerifier verifier;
    verifier = new ZnodeModVerifier(30000, "/" + CLUSTER_NAME + "/EXTERNALVIEW/" + TEST_DB, ZnodePropertyType.ZNODE, "==", destEV);
    testDesc.addVerification(verifier);
    
    stopDummyProcess(10000, "localhost_12918");
    // startDummyProcess(15000, "localhost_12918");

    ZnodeModExecutor executor = new ZnodeModExecutor(testDesc, ZK_ADDR);
    Map<String, Boolean> results = executor.executeTest();

    assertResults(results);
    logger.info("END at " + new Date(System.currentTimeMillis()));
    TestHelper.stopZkServer(zkServer);
  }

  private void assertResults(Map<String, Boolean> results)
  {
    String firstFail = null;
    for (Map.Entry<String, Boolean> entry : results.entrySet())
    {
      logger.info("result:" + entry.getValue() + ", " + entry.getKey());
      if (firstFail == null && entry.getValue() == false)
      {
        firstFail = entry.getKey();
      }
    }
    Assert.assertNull(firstFail);
  }

  private ZNRecord getExternalViewFromIdealState(final ZNRecord idealState)
  {
    ZNRecord externalView = new ZNRecord();

    externalView.setId("TestDB");
    
    for (Map.Entry<String, Map<String, String>> mapEntry : idealState.getMapFields().entrySet())
    {
      mapEntry.getValue().remove("localhost_12918");
      externalView.setMapField(mapEntry.getKey(), mapEntry.getValue());
    }

    return externalView;
  }

  private ZNRecord nextIdealState(final ZNRecord cur, final ZNRecord dest, final int steps) throws PropertyStoreException
  {
    // get a deep copy
    ZNRecord next = _serializer.deserialize(_serializer.serialize(cur));

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

        if (curState == null || !curState.equalsIgnoreCase(destState))
        {
          String[] pair = new String[2];
          pair[0] = new String(resourceKey);
          pair[1] = new String(host);
          list.add(pair);
        }
      }
    }

    // randomly pick up pairs that haven't reached destination state and progress
    for (int i = 0; i < steps; i++)
    {
      int randomInt = RANDOM_GEN.nextInt(list.size());
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
        logger.error("fail to calculate the next ideal state");
      }
      curState = next.getMapField(pair[0]).get(pair[1]);
      if (curState != null && curState.equalsIgnoreCase(destState))
      {
        list.remove(randomInt);
      }
    }

    logger.info("nextIS:" + next);
    return next;
  }

  public void stopDummyProcess(final long stopTime, final String instanceName)
  {
    _executor.schedule(new Runnable()
    {

      @Override
      public void run()
      {
        logger.info("interrupt participant:" + instanceName);
        Thread thread = _threadMap.remove(instanceName).getFirst();
        thread.interrupt();
      }

    }, stopTime, TimeUnit.MILLISECONDS);

  }

  public void startDummyProcess(final long startTime, final String instanceName)
  {
    _executor.schedule(new Runnable()
    {

      @Override
      public void run()
      {
        logger.info("recover participant:" + instanceName);
        Thread thread = startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName, null);
      }

    }, startTime, TimeUnit.MILLISECONDS);
  }

  public Thread startDummyProcess(final String zkAddr,
                                  final String clusterName,
                                  final String instanceName,
                                  final ZkClient zkClient)
  {
    Thread thread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        ClusterManager manager = null;
        try
        {
          manager =  ClusterManagerFactory
              .getZKBasedManagerForParticipant(clusterName, instanceName, zkAddr, zkClient);
          DummyStateModelFactory stateModelFactory = new DummyStateModelFactory(0);
          StateMachineEngine<DummyStateModel> genericStateMachineHandler =
              new StateMachineEngine<DummyStateModel>(stateModelFactory);
          manager.getMessagingService()
                 .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.toString(),
                                                genericStateMachineHandler);

          _threadMap.put(instanceName, new Tuple<Thread, ClusterManager>(Thread.currentThread(),
                                                                         manager));
          Thread.currentThread().join();
        }
        catch (InterruptedException e)
        {
          logger.info("participant:" + instanceName + ", " + Thread.currentThread().getName() + " interrupted");
          if (manager != null)
          {
            manager.disconnect();
          }
        }
        catch (Exception e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    });
    try
    {
      Thread.sleep(500);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    thread.start();

    return thread;
  }

  public Thread startClusterController(final String clusterName,
                                       final String controllerName,
                                       final String zkConnectString,
                                       final String controllerMode,
                                       final ZkClient zkClient)
  {
    Thread thread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        ClusterManager manager = null;

        try
        {
          manager = ClusterManagerMain.startClusterManagerMain(zkConnectString,
                                                               clusterName,
                                                               controllerName,
                                                               controllerMode,
                                                               zkClient);
          _threadMap.put(controllerName, new Tuple<Thread, ClusterManager>(Thread.currentThread(), manager));
          Thread.currentThread().join();
        }
        catch (InterruptedException e)
        {
          logger.info("controller:" + controllerName + ", " + Thread.currentThread().getName() + " interrupted");
          if (manager != null)
          {
            manager.disconnect();
          }
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });

    thread.start();
    return thread;
  }

  private String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }
}

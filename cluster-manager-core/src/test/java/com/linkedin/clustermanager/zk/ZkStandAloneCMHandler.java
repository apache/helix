package com.linkedin.clustermanager.zk;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.TestHelper.DummyProcessResult;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.tools.ClusterSetup;

/**
 * 
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants verify the current states at end
 */

public class ZkStandAloneCMHandler extends ZkTestBase
{
  private static Logger logger = Logger.getLogger(ZkStandAloneCMHandler.class);

  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";
  private static final String TEST_DB = "TestDB";

  protected ZkClient _controllerZkClient;
  protected ZkClient[] _participantZkClients = new ZkClient[NODE_NR];
  protected ClusterSetup _setupTool = null;
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  protected final Map<String, Thread> _threadMap = new HashMap<String, Thread>();
  protected final Map<String, ClusterManager> _managerMap = new HashMap<String, ClusterManager>();

  @BeforeClass
  public void beforeClass() throws Exception
  {
    // logger.info("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    
    String namespace = "/" + CLUSTER_NAME;
    if (_zkClient.exists(namespace))
    {
      _zkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceGroupToCluster(CLUSTER_NAME, TEST_DB, 20, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      String storageNodeName = "localhost:" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);

    // start dummy participants
    Thread thread;
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = "localhost_" + (START_PORT + i);
      if (_threadMap.get(instanceName) != null)
      {
        logger.error("fail to start participant:" + instanceName
            + " because there is already a thread with same instanceName running");
      }
      else
      {
        _participantZkClients[i] = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
        DummyProcessResult result =
            TestHelper.startDummyProcess(ZK_ADDR,
                                         CLUSTER_NAME,
                                         instanceName,
                                         _participantZkClients[i]);
        thread = result._thread;
        _managerMap.put(instanceName, result._manager);
        _threadMap.put(instanceName, thread);
      }
    }

    // start controller
    String controllerName = "controller_0";
    _controllerZkClient = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
    DummyProcessResult startResult =
        TestHelper.startClusterController(CLUSTER_NAME,
                                          controllerName,
                                          ZK_ADDR,
                                          ClusterManagerMain.STANDALONE,
                                          _controllerZkClient);
    _threadMap.put(controllerName, startResult._thread);
    _managerMap.put(controllerName, startResult._manager);
    
    verifyIdealAndCurrentStateTimeout(CLUSTER_NAME);
    
    // Thread.sleep(2000);
    /*
    try
    {
      boolean result = false;
      int i = 0;
      for ( ; i < 24; i++)
      {
        Thread.sleep(2000);
        result = ClusterStateVerifier.verifyClusterStates(ZK_ADDR, CLUSTER_NAME);
        if (result == true)
        {
          break;
        }
      }
      // debug
      System.out.println("ZkStandaloneCMHandler.beforeClass(): wait " + ((i+1) * 2000) 
                         + "ms to verify (" + result + ") cluster:" + CLUSTER_NAME);
      if (result == false)
      {
        System.out.println("ZkStandaloneCMHandler.beforeClass() verification fails");
      }
      Assert.assertTrue(result);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    */
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    // logger.info("END shutting down cluster managers at " + new Date(System.currentTimeMillis()));

    stopThread(_threadMap);
    Thread.sleep(3000);
    // logger.info("END at " + new Date(System.currentTimeMillis()));
    System.out.println("END " + CLASS_NAME + " at "+ new Date(System.currentTimeMillis()));
  }

  private void stopThread(Map<String, Thread> threadMap)
  {
    for (Map.Entry<String, Thread> entry : threadMap.entrySet())
    {
      ClusterManager manager = _managerMap.get(entry.getKey());
      manager.disconnect();
      entry.getValue().interrupt();
    }
  }

  protected void simulateSessionExpiry(ZkClient zkClient) throws IOException,
      InterruptedException
  {
    IZkStateListener listener = new IZkStateListener()
    {
      @Override
      public void handleStateChanged(KeeperState state) throws Exception
      {
        logger.info("In Old connection, state changed:" + state);
      }

      @Override
      public void handleNewSession() throws Exception
      {
        logger.info("In Old connection, new session");
      }
    };
    zkClient.subscribeStateChanges(listener);
    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper oldZookeeper = connection.getZookeeper();
    logger.info("Old sessionId = " + oldZookeeper.getSessionId());

    Watcher watcher = new Watcher()
    {
      @Override
      public void process(WatchedEvent event)
      {
        logger.info("In New connection, process event:" + event);
      }
    };

    ZooKeeper newZookeeper =
        new ZooKeeper(connection.getServers(),
                      oldZookeeper.getSessionTimeout(),
                      watcher,
                      oldZookeeper.getSessionId(),
                      oldZookeeper.getSessionPasswd());
    logger.info("New sessionId = " + newZookeeper.getSessionId());
    // Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    connection = (ZkConnection) zkClient.getConnection();
    oldZookeeper = connection.getZookeeper();
    logger.info("After session expiry sessionId = " + oldZookeeper.getSessionId());
  }

  /*
  protected void stopCurrentLeader(String clusterName)
  {
    String leaderPath =
        CMUtil.getControllerPropertyPath(clusterName, ControllerPropertyType.LEADER);
    final ZkClient zkClient = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
    ZNRecord leaderRecord = zkClient.<ZNRecord> readData(leaderPath);
    Assert.assertTrue(leaderRecord != null);
    String controller = leaderRecord.getSimpleField(ControllerPropertyType.LEADER.toString());
    logger.info("stop current leader:" + controller);
    
    Assert.assertTrue(controller != null);
    ClusterManager manager = _managerMap.remove(controller);
    Assert.assertTrue(manager != null);
    manager.disconnect();
    
    Thread thread = _threadMap.remove(controller);
    Assert.assertTrue(thread != null);
    thread.interrupt();
  }
  */
}

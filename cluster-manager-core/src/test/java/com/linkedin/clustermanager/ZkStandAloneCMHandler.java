package com.linkedin.clustermanager;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.util.CMUtil;

/**
 * 
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants
 * verify the current states at end
 */

public class ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(ZkStandAloneCMHandler.class);
  protected static final String ZK_ADDR = "localhost:2181";
  protected static final String CLUSTER_PREFIX = "ESPRESSO_STORAGE";

  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";
  protected static ZkClient _controllerZkClient;
  protected static ZkClient[] _participantZkClients = new ZkClient[NODE_NR];
  protected ClusterSetup _setupTool = null;
  
  private static final String TEST_DB = "TestDB";
  private ZkServer _zkServer = null;
  // private Map<String, Thread> _participantThreadMap = new HashMap<String, Thread>();
  // private Map<String, Thread> _controllerThreadMap = new HashMap<String, Thread>();
  private Map<String, Thread> _threadMap = new HashMap<String, Thread>();
  
  // static
  @BeforeClass
  public void beforeClass()
  {
    logger.info("START ZkStandAloneCMHandler at " + new Date(System.currentTimeMillis()));
    
    final String clusterName = CLUSTER_PREFIX + "_" + this.getClass().getName();
    _zkServer = TestHelper.startZkSever(ZK_ADDR, "/" + clusterName);
    _setupTool = new ClusterSetup(ZK_ADDR);
    
    // setup storage cluster
    _setupTool.addCluster(clusterName, true);
    _setupTool.addResourceGroupToCluster(clusterName, TEST_DB, 20, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      String storageNodeName = "localhost:" + (START_PORT + i);
      _setupTool.addInstanceToCluster(clusterName, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(clusterName, TEST_DB, 3);
    
    // start dummy participants 
    Thread thread;
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
        _participantZkClients[i] = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
        thread = TestHelper.startDummyProcess(ZK_ADDR, clusterName, 
                       instanceName, _participantZkClients[i]);
        _threadMap.put(instanceName, thread);
      }
    }
    
    // start controller
    String controllerName = "controller_0";
    _controllerZkClient = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
    thread = TestHelper.startClusterController(clusterName, controllerName, ZK_ADDR, 
                       ClusterManagerMain.STANDALONE, _controllerZkClient);
    _threadMap.put(controllerName, thread);
    try
    {
      Thread.sleep(5000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    boolean result = ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, clusterName);
    Assert.assertTrue(result);
    logger.info("cluster:" + clusterName + " starts result:" + result);
  }
  
  @AfterClass
  public void afterClass() throws Exception
  {
    logger.info("END shutting down cluster managers at " + new Date(System.currentTimeMillis()));
    /*
    for (Map.Entry<String, Thread> entry : _controllerThreadMap.entrySet())
    {
      entry.getValue().interrupt();
    }
    */
    stopThread(_threadMap);
    // stopThread(_participantThreadMap);
    Thread.sleep(3000);
    logger.info("END shutting down zk at " + new Date(System.currentTimeMillis()));
    TestHelper.stopZkServer(_zkServer);
  }
  
  private void stopThread(Map<String, Thread> threadMap)
  {
    for (Map.Entry<String, Thread> entry : threadMap.entrySet())
    {
      entry.getValue().interrupt();
    }
  }
  
  protected void simulateSessionExpiry(ZkClient zkClient) 
  throws IOException, InterruptedException
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
    
    ZooKeeper newZookeeper = new ZooKeeper(connection.getServers(),
                  oldZookeeper.getSessionTimeout(), watcher, oldZookeeper.getSessionId(),
                  oldZookeeper.getSessionPasswd());
    logger.info("New sessionId = " + newZookeeper.getSessionId());
    // Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    connection = (ZkConnection) zkClient.getConnection();
    oldZookeeper = connection.getZookeeper();
    logger.info("After session expiry sessionId = " + oldZookeeper.getSessionId());
  }
  
  protected void stopCurrentLeader(String clusterName)
  {
    String leaderPath = CMUtil
        .getControllerPropertyPath(clusterName, ControllerPropertyType.LEADER);
    final ZkClient zkClient = new ZkClient(ZK_ADDR, 3000, 10000, new ZNRecordSerializer());
    ZNRecord leaderRecord = zkClient.<ZNRecord>readData(leaderPath);
    Assert.assertTrue(leaderRecord != null);
    String controller = leaderRecord.getSimpleField(ControllerPropertyType.LEADER.toString());
    logger.info("stop current leader:" + controller);
    Assert.assertTrue(controller != null);
    Thread thread = _threadMap.remove(controller);
    Assert.assertTrue(thread != null);
    thread.interrupt();
  }
  
}

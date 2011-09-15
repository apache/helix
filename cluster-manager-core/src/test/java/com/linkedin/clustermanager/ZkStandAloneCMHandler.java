package com.linkedin.clustermanager;

import java.io.IOException;
import java.util.Date;

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

import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;

/**
 * 
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants
 * verify the current states at end
 */

public class ZkStandAloneCMHandler
{
  private static Logger logger = Logger.getLogger(ZkStandAloneCMHandler.class);
  protected static final String zkAddr = "localhost:2181";
  protected static final String storageCluster = "ESPRESSO_STORAGE";
  private static final String testDB = "TestDB";
  protected static final int storageNodeNr = 5;
  protected static final int startPort = 12918;
  protected static final String stateModel = "MasterSlave";
  private ZkServer _zkServer = null;
  // protected static ZkClient _zkClient;
  protected static ZkClient _controllerZkClient;
  protected static ZkClient[] _participantZkClients = new ZkClient[storageNodeNr];
  protected ClusterSetup _setupTool = null;
  
  // static
  @BeforeClass
  public void beforeClass()
  {
    logger.info("START ZkStandAloneCMHandler at " + new Date(System.currentTimeMillis()));
    _zkServer = TestHelper.startZkSever(zkAddr, "/" + storageCluster);
    // _zkClient = new ZkClient(zkAddr, 1000, 3000);
    _setupTool = new ClusterSetup(zkAddr);
    
    // setup storage cluster
    _setupTool.addCluster(storageCluster, true);
    _setupTool.addResourceGroupToCluster(storageCluster, testDB, 20, stateModel);
    for (int i = 0; i < storageNodeNr; i++)
    {
      String storageNodeName = "localhost:" + (startPort + i);
      _setupTool.addInstanceToCluster(storageCluster, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(storageCluster, testDB, 3);
    
    for (int i = 0; i < storageNodeNr; i++)
    {
      _participantZkClients[i] = new ZkClient(zkAddr, 3000, 10000, new ZNRecordSerializer());
      TestHelper.startDummyProcess(zkAddr, storageCluster, "localhost_" + (startPort + i),
                                   _participantZkClients[i]);
    }
    _controllerZkClient = new ZkClient(zkAddr, 3000, 10000, new ZNRecordSerializer());

    TestHelper.startClusterController(storageCluster, "controller_0", zkAddr, _controllerZkClient);
    try
    {
      Thread.sleep(5000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    boolean result = ClusterStateVerifier.VerifyClusterStates(zkAddr, storageCluster);
    Assert.assertTrue(result);
    logger.info("cluster:" + storageCluster + " starts result:" + result);
  }
  
  @AfterClass
  public void afterClass() throws Exception
  {
    logger.info("END ZkStandAloneCMHandler at " + new Date(System.currentTimeMillis()));
    TestHelper.stopZkServer(_zkServer);
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
    Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    connection = (ZkConnection) zkClient.getConnection();
    oldZookeeper = connection.getZookeeper();
    logger.info("After session expiry sessionId = " + oldZookeeper.getSessionId());
  }
  
}

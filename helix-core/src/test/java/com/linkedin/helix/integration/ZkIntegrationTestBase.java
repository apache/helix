package com.linkedin.helix.integration;

import java.io.IOException;
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
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.util.ZKClientPool;

public class ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(ZkIntegrationTestBase.class);

  protected static ZkServer _zkServer;
  protected static ZkClient _gZkClient;

  public static final String ZK_ADDR = "localhost:2183";
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  protected final String CONTROLLER_PREFIX = "controller";
  protected final String PARTICIPANT_PREFIX = "localhost";

  @BeforeSuite
  public void beforeSuite() throws Exception
  {
    _zkServer = TestHelper.startZkSever(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    ZKClientPool.reset();

    _gZkClient = new ZkClient(ZK_ADDR);
    _gZkClient.setZkSerializer(new ZNRecordSerializer());
  }

  @AfterSuite
  public void afterSuite()
  {
    ZKClientPool.reset();
    TestHelper.stopZkServer(_zkServer);
    // _zkServer = null;
    _gZkClient.close();
  }

  protected String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

  protected String getCurrentLeader(ZkClient zkClient, String clusterName)
  {
    String leaderPath = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);

    ZNRecord leaderRecord = zkClient.<ZNRecord> readData(leaderPath, true);
    if (leaderRecord == null)
    {
      return null;
    }

    String leader = leaderRecord.getSimpleField(PropertyType.LEADER.toString());
    return leader;
  }

  /**
   * Stop current leader and returns the new leader
   *
   * @param zkClient
   * @param clusterName
   * @param startCMResultMap
   * @return
   */
  protected String stopCurrentLeader(ZkClient zkClient,
                                     String clusterName,
                                     Map<String, StartCMResult> startCMResultMap)
  {
    String leader = getCurrentLeader(zkClient, clusterName);
    Assert.assertTrue(leader != null);
    System.out.println("stop leader:" + leader + " in " + clusterName);
    Assert.assertTrue(leader != null);

    StartCMResult result = startCMResultMap.remove(leader);
    Assert.assertTrue(result._manager != null);
    result._manager.disconnect();

    Assert.assertTrue(result._thread != null);
    result._thread.interrupt();

    boolean isNewLeaderElected = false;
    String newLeader = null;
    try
    {
      for (int i = 0; i < 5; i++)
      {
        Thread.sleep(1000);
        newLeader = getCurrentLeader(zkClient, clusterName);
        if (!newLeader.equals(leader))
        {
          isNewLeaderElected = true;
          System.out.println("new leader elected: " + newLeader + " in " + clusterName);
          break;
        }
      }
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    if (isNewLeaderElected == false)
    {
      System.out.println("fail to elect a new leader in " + clusterName);
    }
    AssertJUnit.assertTrue(isNewLeaderElected);
    return newLeader;
  }

  /**
   * simulate session expiry
   *
   * @param zkConnection
   * @throws IOException
   * @throws InterruptedException
   */
  protected void simulateSessionExpiry(ZkConnection zkConnection) throws IOException,
      InterruptedException
  {
    ZooKeeper oldZookeeper = zkConnection.getZookeeper();
    LOG.info("Old sessionId = " + oldZookeeper.getSessionId());

    Watcher watcher = new Watcher()
    {
      @Override
      public void process(WatchedEvent event)
      {
        LOG.info("In New connection, process event:" + event);
      }
    };

    ZooKeeper newZookeeper =
        new ZooKeeper(zkConnection.getServers(),
                      oldZookeeper.getSessionTimeout(),
                      watcher,
                      oldZookeeper.getSessionId(),
                      oldZookeeper.getSessionPasswd());
    LOG.info("New sessionId = " + newZookeeper.getSessionId());
    // Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    oldZookeeper = zkConnection.getZookeeper();
    LOG.info("After session expiry sessionId = " + oldZookeeper.getSessionId());
  }

  protected static void simulateSessionExpiry(ZkClient zkClient) throws IOException,
      InterruptedException
  {
    IZkStateListener listener = new IZkStateListener()
    {
      @Override
      public void handleStateChanged(KeeperState state) throws Exception
      {
        LOG.info("In Old connection, state changed:" + state);
      }

      @Override
      public void handleNewSession() throws Exception
      {
        LOG.info("In Old connection, new session");
      }
    };
    zkClient.subscribeStateChanges(listener);
    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper oldZookeeper = connection.getZookeeper();
    LOG.info("Old sessionId = " + oldZookeeper.getSessionId());

    Watcher watcher = new Watcher()
    {
      @Override
      public void process(WatchedEvent event)
      {
        LOG.info("In New connection, process event:" + event);
      }
    };

    ZooKeeper newZookeeper =
        new ZooKeeper(connection.getServers(),
                      oldZookeeper.getSessionTimeout(),
                      watcher,
                      oldZookeeper.getSessionId(),
                      oldZookeeper.getSessionPasswd());
    LOG.info("New sessionId = " + newZookeeper.getSessionId());
    // Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    connection = (ZkConnection) zkClient.getConnection();
    oldZookeeper = connection.getZookeeper();
    LOG.info("After session expiry sessionId = " + oldZookeeper.getSessionId());
  }
}

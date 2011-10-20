package com.linkedin.clustermanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.util.CMUtil;
import com.linkedin.clustermanager.util.ZKClientPool;

// TODO merge code with ZkIntegrationTestBase
public class ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(ZkUnitTestBase.class);

  protected static ZkServer _zkServer = null;
  protected static ZkClient _zkClient = null;

  public static final String ZK_ADDR = "localhost:2185";
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  @BeforeSuite(groups =
  { "unitTest" })
  public void beforeSuite()
  {
    _zkServer = TestHelper.startZkSever(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    ZKClientPool.reset();

    _zkClient = ZKClientPool.getZkClient(ZK_ADDR);
    AssertJUnit.assertTrue(_zkClient != null);
  }

  @AfterSuite(groups =
  { "unitTest" })
  public void afterSuite()
  {
    ZKClientPool.reset();
    _zkClient.close();
    _zkClient = null;
    TestHelper.stopZkServer(_zkServer);
    _zkServer = null;
  }

  protected String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

  protected String getCurrentLeader(String clusterName)
  {
    String leaderPath = CMUtil.getControllerPropertyPath(clusterName,
        PropertyType.LEADER);
    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(leaderPath);
    if (leaderRecord == null)
    {
      return null;
    }

    String leader = leaderRecord.getSimpleField(PropertyType.LEADER.toString());
    return leader;
  }

  protected void stopCurrentLeader(String clusterName,
      Map<String, Thread> threadMap, Map<String, ClusterManager> managerMap)
  {
    String leader = getCurrentLeader(clusterName);
    Assert.assertTrue(leader != null);
    System.out.println("stop leader:" + leader + " in " + clusterName);
    Assert.assertTrue(leader != null);

    ClusterManager manager = managerMap.remove(leader);
    Assert.assertTrue(manager != null);
    manager.disconnect();

    Thread thread = threadMap.remove(leader);
    Assert.assertTrue(thread != null);
    thread.interrupt();

    boolean isNewLeaderElected = false;
    try
    {
      // Thread.sleep(2000);
      for (int i = 0; i < 5; i++)
      {
        Thread.sleep(1000);
        String newLeader = getCurrentLeader(clusterName);
        if (!newLeader.equals(leader))
        {
          isNewLeaderElected = true;
          System.out.println("new leader elected: " + newLeader + " in "
              + clusterName);
          break;
        }
      }
    } catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    if (isNewLeaderElected == false)
    {
      System.out
          .println("fail to elect a new leader elected in " + clusterName);
    }
    AssertJUnit.assertTrue(isNewLeaderElected);
  }

  protected void verifyIdealAndCurrentStateTimeout(String clusterName)
  {
    List<String> clusterNames = new ArrayList<String>();
    clusterNames.add(clusterName);
    verifyIdealAndCurrentStateTimeout(clusterNames);
  }

  protected void verifyIdealAndCurrentStateTimeout(List<String> clusterNames)
  {
    try
    {
      boolean result = false;
      int i = 0;
      for (; i < 24; i++)
      {
        Thread.sleep(2000);
        result = verifyIdealAndCurrentState(clusterNames);
        if (result == true)
        {
          break;
        }
      }
      // debug
      System.out.println("verifyIdealAndCurrentState(): wait "
          + ((i + 1) * 2000) + "ms to verify (" + result + ") clusters:"
          + Arrays.toString(clusterNames.toArray()));

      if (result == false)
      {
        System.out.println("verifyIdealAndCurrentState() fails for clusters:"
            + Arrays.toString(clusterNames.toArray()));
      }
      AssertJUnit.assertTrue(result);
    } catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private boolean verifyIdealAndCurrentState(List<String> clusterNames)
  {
    for (String clusterName : clusterNames)
    {
      boolean result = ClusterStateVerifier.verifyClusterStates(ZK_ADDR,
          clusterName);
      LOG.info("verify cluster: " + clusterName + ", result: " + result);
      if (result == false)
      {
        return result;
      }
    }
    return true;
  }

  protected void verifyEmtpyCurrentStateTimeout(String clusterName,
      String resourceGroupName, List<String> instanceNames)
  {
    try
    {
      boolean result = false;
      int i = 0;
      for (; i < 24; i++)
      {
        Thread.sleep(2000);
        result = verifyEmptyCurrentState(clusterName, resourceGroupName,
            instanceNames);
        if (result == true)
        {
          break;
        }
      }
      // debug
      System.out.println("verifyEmtpyCurrentState(): wait " + ((i + 1) * 2000)
          + "ms to verify (" + result + ") cluster:" + clusterName);
      if (result == false)
      {
        System.out.println("verifyEmtpyCurrentState() fails");
      }
      Assert.assertTrue(result);
    } catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private boolean verifyEmptyCurrentState(String clusterName,
      String resourceGroupName, List<String> instanceNames)
  {
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);

    for (String instanceName : instanceNames)
    {
      String path = CMUtil.getInstancePropertyPath(clusterName, instanceName,
          PropertyType.CURRENTSTATES);

      List<String> subPaths = accessor.getChildNames(
          PropertyType.CURRENTSTATES, instanceName);

      for (String previousSessionId : subPaths)
      {
        if (_zkClient.exists(path + "/" + previousSessionId + "/"
            + resourceGroupName))
        {
          ZNRecord previousCurrentState = accessor.getProperty(
              PropertyType.CURRENTSTATES, instanceName, previousSessionId,
              resourceGroupName);

          if (previousCurrentState.getMapFields().size() != 0)
          {
            return false;
          }
        }
      }
    }
    return true;
  }

  protected void simulateSessionExpiry(ZkConnection zkConnection)
      throws IOException, InterruptedException
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

    ZooKeeper newZookeeper = new ZooKeeper(zkConnection.getServers(),
        oldZookeeper.getSessionTimeout(), watcher, oldZookeeper.getSessionId(),
        oldZookeeper.getSessionPasswd());
    LOG.info("New sessionId = " + newZookeeper.getSessionId());
    // Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    oldZookeeper = zkConnection.getZookeeper();
    LOG.info("After session expiry sessionId = " + oldZookeeper.getSessionId());
  }

  protected void simulateSessionExpiry(ZkClient zkClient) throws IOException,
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

    ZooKeeper newZookeeper = new ZooKeeper(connection.getServers(),
        oldZookeeper.getSessionTimeout(), watcher, oldZookeeper.getSessionId(),
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

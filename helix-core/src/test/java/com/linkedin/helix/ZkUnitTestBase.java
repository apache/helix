package com.linkedin.helix;

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

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.util.HelixUtil;

// TODO merge code with ZkIntegrationTestBase
public class ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(ZkUnitTestBase.class);
  protected static ZkServer _zkServer = null;

  public static final String ZK_ADDR = "localhost:2185";
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  @BeforeSuite(alwaysRun = true)
  public void beforeSuite() throws Exception
  {
    _zkServer = TestHelper.startZkSever(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
  }

  @AfterSuite(alwaysRun = true)
  public void afterTest()
  {
    TestHelper.stopZkServer(_zkServer);
    _zkServer = null;
  }


  protected String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

  protected String getCurrentLeader(ZkClient zkClient, String clusterName)
  {
    String leaderPath = HelixUtil.getControllerPropertyPath(clusterName,
        PropertyType.LEADER);
    ZNRecord leaderRecord = zkClient.<ZNRecord> readData(leaderPath);
    if (leaderRecord == null)
    {
      return null;
    }

    String leader = leaderRecord.getSimpleField(PropertyType.LEADER.toString());
    return leader;
  }

  protected void stopCurrentLeader(ZkClient zkClient, String clusterName,
      Map<String, Thread> threadMap, Map<String, HelixManager> managerMap)
  {
    String leader = getCurrentLeader(zkClient, clusterName);
    Assert.assertTrue(leader != null);
    System.out.println("stop leader:" + leader + " in " + clusterName);
    Assert.assertTrue(leader != null);

    HelixManager manager = managerMap.remove(leader);
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
        String newLeader = getCurrentLeader(zkClient, clusterName);
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

  public void verifyInstance(ZkClient zkClient, String clusterName, String instance, boolean wantExists)
	{
		String instanceConfigsPath = HelixUtil.getConfigPath(clusterName);
	    String instanceConfigPath = instanceConfigsPath + "/" + instance;
	    String instancePath = HelixUtil.getInstancePath(clusterName, instance);
	    AssertJUnit.assertEquals(wantExists, zkClient.exists(instanceConfigPath));
	    AssertJUnit.assertEquals(wantExists, zkClient.exists(instancePath));
	}

	public void verifyResource(ZkClient zkClient, String clusterName, String resource, boolean wantExists)
	{
		String resourcePath = HelixUtil.getIdealStatePath(clusterName)+"/"+resource;
		AssertJUnit.assertEquals(wantExists, zkClient.exists(resourcePath));
	}

	public void verifyEnabled(ZkClient zkClient, String clusterName, String instance, boolean wantEnabled)
	{
	    DataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
	    InstanceConfig config = accessor.getProperty(InstanceConfig.class, PropertyType.CONFIGS, instance);
	    AssertJUnit.assertEquals(wantEnabled, config.getInstanceEnabled());
	}


	public void verifyReplication(ZkClient zkClient, String clusterName, String resource, int repl)
	{
		 DataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
		 IdealState idealState = accessor.getProperty(IdealState.class, PropertyType.IDEALSTATES, resource);
		 for (String partitionName : idealState.getPartitionSet())
		 {
			 AssertJUnit.assertEquals(repl, idealState.getInstanceStateMap(partitionName).size());
		 }
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

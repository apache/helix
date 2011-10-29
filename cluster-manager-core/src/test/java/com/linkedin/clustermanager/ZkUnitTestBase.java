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

import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.util.CMUtil;

// TODO merge code with ZkIntegrationTestBase
public class ZkUnitTestBase
{
  private static Logger LOG = Logger.getLogger(ZkUnitTestBase.class);

  protected static ZkServer _zkServer = null;
  // protected static ZkClient _zkClient = null;

  public static final String ZK_ADDR = "localhost:2185";
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  
  @BeforeSuite (alwaysRun = true)
  public void beforeSuite() throws Exception
  {
  	// System.out.println("START beforeSuite() in UnitTest");
    _zkServer = TestHelper.startZkSever(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    // ZKClientPool.reset();

    // _zkClient = ZKClientPool.getZkClient(ZK_ADDR);
    // AssertJUnit.assertTrue(_zkClient != null);
  }

  @AfterSuite (alwaysRun = true)
  public void afterTest()
  {
  	// System.out.println("START afterSuite() in UnitTest");
    // ZKClientPool.reset();
    // _zkClient.close();
    // _zkClient = null;
  	// System.err.println("shutting down zkserver at " + _zkServer.getPort());
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
    String leaderPath = CMUtil.getControllerPropertyPath(clusterName,
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
      Map<String, Thread> threadMap, Map<String, ClusterManager> managerMap)
  {
    String leader = getCurrentLeader(zkClient, clusterName);
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

  protected void verifyEmtpyCurrentStateTimeout(ZkClient zkClient, String clusterName,
      String resourceGroupName, List<String> instanceNames)
  {
    try
    {
      boolean result = false;
      int i = 0;
      for (; i < 24; i++)
      {
        Thread.sleep(2000);
        result = verifyEmptyCurrentState(zkClient, clusterName, resourceGroupName,
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

  private boolean verifyEmptyCurrentState(ZkClient zkClient, String clusterName,
      String resourceGroupName, List<String> instanceNames)
  {
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);

    for (String instanceName : instanceNames)
    {
      String path = CMUtil.getInstancePropertyPath(clusterName, instanceName,
          PropertyType.CURRENTSTATES);

      List<String> subPaths = accessor.getChildNames(
          PropertyType.CURRENTSTATES, instanceName);

      for (String previousSessionId : subPaths)
      {
        if (zkClient.exists(path + "/" + previousSessionId + "/"
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

  public void verifyInstance(ZkClient zkClient, String clusterName, String instance, boolean wantExists)
	{
		String instanceConfigsPath = CMUtil.getConfigPath(clusterName);
	    String instanceConfigPath = instanceConfigsPath + "/" + instance;
	    String instancePath = CMUtil.getInstancePath(clusterName, instance);
	    AssertJUnit.assertEquals(wantExists, zkClient.exists(instanceConfigPath));
	    AssertJUnit.assertEquals(wantExists, zkClient.exists(instancePath));
	}
	
	public void verifyResource(ZkClient zkClient, String clusterName, String resource, boolean wantExists) 
	{
		String resourcePath = CMUtil.getIdealStatePath(clusterName)+"/"+resource;
		AssertJUnit.assertEquals(wantExists, zkClient.exists(resourcePath));
	}
	
	public void verifyEnabled(ZkClient zkClient, String clusterName, String instance, boolean wantEnabled) {
	    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
	    ZNRecord nodeConfig = accessor.getProperty(PropertyType.CONFIGS, instance);
	    boolean isEnabled = Boolean.parseBoolean(nodeConfig.getSimpleField(
	    		InstanceConfigProperty.ENABLED.toString()));
	    AssertJUnit.assertEquals(wantEnabled, isEnabled);
	}
	
	
	public void verifyReplication(ZkClient zkClient, String clusterName, String resource, int repl) {
		//String resourcePath = CMUtil.getIdealStatePath(CLUSTER_NAME)+"/"+TEST_DB;
		 ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, zkClient);
		 ZNRecord nodeConfig = accessor.getProperty(PropertyType.IDEALSTATES, resource);
		 //get location map for each partition
		 Map<String, Map<String, String>> partitionMap = nodeConfig.getMapFields();
		 //for each partition's map, verify map has repl entries
		 for (String partition : partitionMap.keySet()) {
			 Map<String, String> partitionLocs = nodeConfig.getMapField(partition);
			 AssertJUnit.assertEquals(repl, partitionLocs.size());
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

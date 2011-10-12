package com.linkedin.clustermanager.zk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.ControllerPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.TestHelper;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.util.CMUtil;
import com.linkedin.clustermanager.util.ZKClientPool;

public class ZkTestBase
{
  private static Logger LOG = Logger.getLogger(ZkTestBase.class);

  private static ZkServer _zkServer = null;
  protected static ZkClient _zkClient = null;

  protected static final String ZK_ADDR = "localhost:2183";
  protected static final String CLUSTER_PREFIX = "ESPRESSO_STORAGE";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  @BeforeSuite
  public void beforeSuite()
  {
    _zkServer = TestHelper.startZkSever(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);

    _zkClient = ZKClientPool.getZkClient(ZK_ADDR);
    AssertJUnit.assertTrue(_zkClient != null);
  }

  @AfterSuite
  public void afterSuite()
  {
    TestHelper.stopZkServer(_zkServer);
  }

  protected String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

  protected String getCurrentLeader(String clusterName)
  {
    String leaderPath =
        CMUtil.getControllerPropertyPath(clusterName, ControllerPropertyType.LEADER);
    ZNRecord leaderRecord = _zkClient.<ZNRecord> readData(leaderPath);
    if (leaderRecord == null)
    {
      return null;
    }

    String leader = leaderRecord.getSimpleField(ControllerPropertyType.LEADER.toString());
    return leader;
  }

  protected void stopCurrentLeader(String clusterName,
                                   Map<String, Thread> threadMap,
                                   Map<String, ClusterManager> managerMap)
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
      Thread.sleep(2000);
      for (int i = 0; i < 5; i++)
      {
        Thread.sleep(1000);
        String newLeader = getCurrentLeader(clusterName);
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
      System.out.println("fail to elect a new leader elected in " + clusterName);
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
      System.out.println("verifyIdealAndCurrentState(): wait " + ((i + 1) * 2000)
          + "ms to verify (" + result + ") clusters:" + Arrays.toString(clusterNames.toArray()));

      if (result == false)
      {
        System.out.println("verifyIdealAndCurrentState() fails for clusters:"
            + Arrays.toString(clusterNames.toArray()));
      }
      AssertJUnit.assertTrue(result);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private boolean verifyIdealAndCurrentState(List<String> clusterNames)
  {
    for (String clusterName : clusterNames)
    {
      boolean result = ClusterStateVerifier.verifyClusterStates(ZK_ADDR, clusterName);
      LOG.info("verify cluster: " + clusterName + ", result: " + result);
      if (result == false)
      {
        return result;
      }
    }
    return true;
  }

  protected void verifyEmtpyCurrentStateTimeout(String clusterName,
                                                String resourceGroupName,
                                                List<String> instanceNames)
  {
    try
    {
      boolean result = false;
      int i = 0;
      for (; i < 24; i++)
      {
        Thread.sleep(2000);
        result = verifyEmptyCurrentState(clusterName, resourceGroupName, instanceNames);
        if (result == true)
        {
          break;
        }
      }
      // debug
      System.out.println("verifyEmtpyCurrentState(): wait " + ((i + 1) * 2000) + "ms to verify ("
          + result + ") cluster:" + clusterName);
      if (result == false)
      {
        System.out.println("verifyEmtpyCurrentState() fails");
      }
      Assert.assertTrue(result);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private boolean verifyEmptyCurrentState(String clusterName,
                                          String resourceGroupName,
                                          List<String> instanceNames)
  {
    ClusterDataAccessor accessor = new ZKDataAccessor(clusterName, _zkClient);

    for (String instanceName : instanceNames)
    {
      String path =
          CMUtil.getInstancePropertyPath(clusterName,
                                         instanceName,
                                         InstancePropertyType.CURRENTSTATES);

      List<String> subPaths =
          accessor.getInstancePropertySubPaths(instanceName, InstancePropertyType.CURRENTSTATES);

      for (String previousSessionId : subPaths)
      {
        if (_zkClient.exists(path + "/" + previousSessionId + "/" + resourceGroupName))
        {
          ZNRecord previousCurrentState =
              accessor.getInstanceProperty(instanceName,
                                           InstancePropertyType.CURRENTSTATES,
                                           previousSessionId,
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
}

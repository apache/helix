package com.linkedin.helix;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.manager.zk.ZKHelixDataAccessor;
import com.linkedin.helix.manager.zk.ZKHelixManager;
import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.ExternalView;

public class ZkTestHelper
{
  private static Logger LOG = Logger.getLogger(ZkTestHelper.class);

  static
  {
    // Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  // zkClusterManager that exposes zkclient
  public static class TestZkHelixManager extends ZKHelixManager
  {

    public TestZkHelixManager(String clusterName,
                              String instanceName,
                              InstanceType instanceType,
                              String zkConnectString) throws Exception
    {
      super(clusterName, instanceName, instanceType, zkConnectString);
      // TODO Auto-generated constructor stub
    }

    public ZkClient getZkClient()
    {
      return _zkClient;
    }

  }

  public static void expireSession(final ZkClient zkClient) throws Exception
  {
    final CountDownLatch waitExpire = new CountDownLatch(1);

    IZkStateListener listener = new IZkStateListener()
    {
      @Override
      public void handleStateChanged(KeeperState state) throws Exception
      {
        // System.err.println("handleStateChanged. state: " + state);
      }

      @Override
      public void handleNewSession() throws Exception
      {
        // make sure zkclient is connected again
        zkClient.waitUntilConnected();

        ZkConnection connection = ((ZkConnection) zkClient.getConnection());
        ZooKeeper curZookeeper = connection.getZookeeper();

        LOG.info("handleNewSession. sessionId: "
            + Long.toHexString(curZookeeper.getSessionId()));
        waitExpire.countDown();
      }
    };

    zkClient.subscribeStateChanges(listener);

    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper curZookeeper = connection.getZookeeper();
    LOG.info("Before expiry. sessionId: " + Long.toHexString(curZookeeper.getSessionId()));

    Watcher watcher = new Watcher()
    {
      @Override
      public void process(WatchedEvent event)
      {
        LOG.info("Process watchEvent: " + event);
      }
    };

    final ZooKeeper dupZookeeper =
        new ZooKeeper(connection.getServers(),
                      curZookeeper.getSessionTimeout(),
                      watcher,
                      curZookeeper.getSessionId(),
                      curZookeeper.getSessionPasswd());
    // wait until connected, then close
    while (dupZookeeper.getState() != States.CONNECTED)
    {
      Thread.sleep(10);
    }
    dupZookeeper.close();

    // make sure session expiry really happens
    waitExpire.await();
    zkClient.unsubscribeStateChanges(listener);

    connection = (ZkConnection) zkClient.getConnection();
    curZookeeper = connection.getZookeeper();

    // System.err.println("zk: " + oldZookeeper);
    LOG.info("After expiry. sessionId: " + Long.toHexString(curZookeeper.getSessionId()));
  }

  /*
   * stateMap: partition->instance->state
   */
  public static boolean verifyState(ZkClient zkclient,
                                    String clusterName,
                                    String resourceName,
                                    Map<String, Map<String, String>> expectStateMap,
                                    String op)
  {
    boolean result = true;
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(zkclient);
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();

    ExternalView extView = accessor.getProperty(keyBuilder.externalView(resourceName));
    Map<String, Map<String, String>> actualStateMap = extView.getRecord().getMapFields();
    for (String partition : actualStateMap.keySet())
    {
      for (String expectPartiton : expectStateMap.keySet())
      {
        if (!partition.matches(expectPartiton))
        {
          continue;
        }

        Map<String, String> actualInstanceStateMap = actualStateMap.get(partition);
        Map<String, String> expectInstanceStateMap = expectStateMap.get(expectPartiton);
        for (String instance : actualInstanceStateMap.keySet())
        {
          for (String expectInstance : expectStateMap.get(expectPartiton).keySet())
          {
            if (!instance.matches(expectInstance))
            {
              continue;
            }

            String actualState = actualInstanceStateMap.get(instance);
            String expectState = expectInstanceStateMap.get(expectInstance);
            boolean equals = expectState.equals(actualState);
            if (op.equals("==") && !equals || op.equals("!=") && equals)
            {
              System.out.println(partition + "/" + instance
                  + " state mismatch. actual state: " + actualState + ", but expect: "
                  + expectState + ", op: " + op);
              result = false;
            }

          }
        }

      }
    }
    return result;
  }
}

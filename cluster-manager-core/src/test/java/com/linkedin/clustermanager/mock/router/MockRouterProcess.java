package com.linkedin.clustermanager.mock.router;

import java.util.List;

import org.I0Itec.zkclient.IDefaultNameSpace;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import org.I0Itec.zkclient.ZkServer;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.spectator.RoutingTableProvider;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.util.CMUtil;

/**
 * A MockRouter process to demonstrate the integration with cluster manager.
 * This uses Zookeeper in local mode and runs at port 2188
 * 
 * @author kgopalak
 * 
 */
public class MockRouterProcess
{
  private static final int port = 2188;
  static long runId = System.currentTimeMillis();
  private static final String dataDir = "/tmp/zkDataDir-" + runId;

  private static final String logDir = "/tmp/zkLogDir-" + runId;

  static String clusterName = "mock-cluster-" + runId;

  static String zkConnectString = "localhost:2188";

  private RoutingTableProvider _routingTableProvider;
  private static ZkServer zkServer;

  public MockRouterProcess()
  {
    _routingTableProvider = new RoutingTableProvider();
  }

  public static void main(String[] args) throws Exception
  {
    setup();
    zkServer.getZkClient().setZkSerializer(new ZNRecordSerializer());
    ZNRecord record = zkServer.getZkClient().readData(
        CMUtil.getIdealStatePath(clusterName, "TestDB"));

    String externalViewPath = CMUtil.getExternalViewPath(clusterName, "TestDB");

    MockRouterProcess process = new MockRouterProcess();
    process.start();
    //try to route, there is no master or slave available
    process.routeRequest("TestDB", "TestDB_1");
    
    //update the externalview on zookeeper
    zkServer.getZkClient().createPersistent(externalViewPath,record);
    //sleep for sometime so that the ZK Callback is received.
    Thread.sleep(1000);
    process.routeRequest("TestDB", "TestDB_1");
    System.exit(1);
  }

  private static void setup()
  {

    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient client)
      {
        client.deleteRecursive("/" + clusterName);

      }
    };

    zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();
    ClusterSetup clusterSetup = new ClusterSetup(zkConnectString);
    clusterSetup.setupTestCluster(clusterName);
    try
    {
      Thread.sleep(1000);
    } catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }

  public void routeRequest(String database, String partition)
  {
    List<InstanceConfig> masters;
    List<InstanceConfig> slaves;
    masters = _routingTableProvider.getInstances(database, partition, "MASTER");
    if (masters != null && !masters.isEmpty())
    {
      System.out.println("Available masters to route request");
      for (InstanceConfig config : masters)
      {
        System.out.println("HostName:" + config.getHostName() + " Port:"
            + config.getPort());
      }
    } else
    {
      System.out.println("No masters available to route request");
    }
    slaves = _routingTableProvider.getInstances(database, partition, "SLAVE");
    if (slaves != null && !slaves.isEmpty())
    {
      System.out.println("Available slaves to route request");
      for (InstanceConfig config : slaves)
      {
        System.out.println("HostName:" + config.getHostName() + " Port:"
            + config.getPort());
      }
    } else
    {
      System.out.println("No slaves available to route request");
    }
  }

  public void start()
  {

    try
    {
      ClusterManager manager = ClusterManagerFactory
          .getZKBasedManagerForSpectator(clusterName, zkConnectString);

      manager.connect();
      manager.addExternalViewChangeListener(_routingTableProvider);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}

package com.linkedin.clustermanager;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.mock.storage.DummyProcess;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;
import com.linkedin.clustermanager.tools.StateModelConfigGenerator;
import com.linkedin.clustermanager.util.CMUtil;
import com.linkedin.clustermanager.agent.zk.ZkClient;

public class TestDropResource
{
  void VerifyEmptyCurrentState(ClusterDataAccessor accessor, ZkClient client, String clusterName, String instanceName, String dbName)
  {
    String path = CMUtil.getInstancePropertyPath(clusterName, instanceName, InstancePropertyType.CURRENTSTATES);
    List<String> subPaths = accessor.getInstancePropertySubPaths(instanceName, InstancePropertyType.CURRENTSTATES);
    
    for(String previousSessionId : subPaths)
    {
      if(client.exists(path+"/"+previousSessionId+"/" + dbName))
      {
        ZNRecord previousCurrentState = accessor.getInstanceProperty(instanceName, InstancePropertyType.CURRENTSTATES, previousSessionId, dbName);
        AssertJUnit.assertTrue(previousCurrentState.getMapFields().size() == 0);
      }
    }
  }
  
  @Test
  public void testInvocation() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.ERROR);
    String logDir = "/tmp/logs";
    String dataDir = "/tmp/dataDir";
    new File(dataDir).delete();
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(org.I0Itec.zkclient.ZkClient zkClient)
      {
        zkClient.deleteRecursive("/ESPRESSO_STORAGE");
        zkClient.deleteRecursive("/relay-cluster-12345");
      }
    };
    int port = 2181;
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();
    ZkClient zkClient = new ZkClient("localhost:2181");
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ClusterDataAccessor accessor = new ZKDataAccessor("ESPRESSO_STORAGE", zkClient);
    ClusterSetup setup = new ClusterSetup("localhost:" + port);
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addCluster ESPRESSO_STORAGE"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addCluster relay-cluster-12345"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addResourceGroup ESPRESSO_STORAGE db-12345 100 MasterSlave"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode ESPRESSO_STORAGE localhost:8900"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode ESPRESSO_STORAGE localhost:8901"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode ESPRESSO_STORAGE localhost:8902"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode ESPRESSO_STORAGE localhost:8903"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode ESPRESSO_STORAGE localhost:8904"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -rebalance ESPRESSO_STORAGE db-12345 2"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8900"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8901"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8902"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8903"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8904"));
    startClusterManager(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE"));
    Thread.sleep(15000);
    //Thread.currentThread().join();
    setup.dropResourceGroupToCluster("ESPRESSO_STORAGE", "db-12345");
    Thread.sleep(15000);
    VerifyEmptyCurrentState(accessor, zkClient, "ESPRESSO_STORAGE", "localhost_8900", "db-12345");
    VerifyEmptyCurrentState(accessor, zkClient, "ESPRESSO_STORAGE", "localhost_8901", "db-12345");
    VerifyEmptyCurrentState(accessor, zkClient, "ESPRESSO_STORAGE", "localhost_8902", "db-12345");
    VerifyEmptyCurrentState(accessor, zkClient, "ESPRESSO_STORAGE", "localhost_8903", "db-12345");
    VerifyEmptyCurrentState(accessor, zkClient, "ESPRESSO_STORAGE", "localhost_8904", "db-12345");
    
    zkServer.shutdown();
  }

  private static void startClusterManager(final String[] args)
  {
    Thread t = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          ClusterManagerMain.main(args);
        } catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });
    t.start();
  }

  private static void startDummyProcess(final String[] args)
  {
    Thread t = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          DummyProcess.main(args);
        } catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });
    t.start();
    try
    {
      Thread.sleep(1000);
    } catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static String[] createArgs(String str)
  {
    String[] split = str.split("[ ]+");
    System.out.println(Arrays.toString(split));
    return split;
  }
}

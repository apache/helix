package com.linkedin.clustermanager;

import java.io.File;
import java.util.Arrays;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.mock.storage.DummyProcess;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;

/**
 * This is a simple integration test. We will use this until we have framework
 * which helps us write integration tests easily
 * 
 * @author kgopalak
 * 
 */
public class IntegrationTest
{

  public static void main(String[] args) throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.ERROR);
    String logDir = "/tmp/logs";
    String dataDir = "/tmp/dataDir";
    new File(dataDir).delete();
    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace()
    {
      @Override
      public void createDefaultNameSpace(ZkClient zkClient)
      {
        zkClient.deleteRecursive("/storage-cluster-12345");
        zkClient.deleteRecursive("/relay-cluster-12345");
      }
    };
    int port = 2188;
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -addCluster storage-cluster-12345"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -addCluster relay-cluster-12345"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -addDatabase storage-cluster-12345 db-12345 120"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -addNode storage-cluster-12345 localhost:8900"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -addNode storage-cluster-12345 localhost:8901"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -addNode storage-cluster-12345 localhost:8902"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -addNode storage-cluster-12345 localhost:8903"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -addNode storage-cluster-12345 localhost:8904"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2188 -rebalance storage-cluster-12345 db-12345 3"));
    startDummyProcess(createArgs("-zkSvr localhost:2188 -cluster storage-cluster-12345 -host localhost -port 8900"));
    startDummyProcess(createArgs("-zkSvr localhost:2188 -cluster storage-cluster-12345 -host localhost -port 8901"));
    startDummyProcess(createArgs("-zkSvr localhost:2188 -cluster storage-cluster-12345 -host localhost -port 8902"));
    startDummyProcess(createArgs("-zkSvr localhost:2188 -cluster storage-cluster-12345 -host localhost -port 8903"));
    startDummyProcess(createArgs("-zkSvr localhost:2188 -cluster storage-cluster-12345 -host localhost -port 8904"));
    startClusterManager(createArgs("-zkSvr localhost:2188 -cluster storage-cluster-12345"));
    Thread.sleep(60000);
    ClusterStateVerifier
        .main(createArgs("-zkSvr localhost:2188 -cluster storage-cluster-12345"));
    // zkServer.shutdown();
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

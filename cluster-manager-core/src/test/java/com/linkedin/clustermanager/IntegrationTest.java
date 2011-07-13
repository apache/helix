package com.linkedin.clustermanager;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;
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
  @Test
  public void testInvocation() throws Exception
  //public static void main(String[] args) throws Exception
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
        zkClient.deleteRecursive("/test-cluster");
        zkClient.deleteRecursive("/relay-cluster-12345");
      }
    };
    int port = 2181;
    ZkServer zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
    zkServer.start();
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addCluster test-cluster"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addCluster relay-cluster-12345"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addDatabase test-cluster db-12345 120"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode test-cluster localhost:8900"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode test-cluster localhost:8901"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode test-cluster localhost:8902"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode test-cluster localhost:8903"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addNode test-cluster localhost:8904"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -rebalance test-cluster db-12345 3"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster test-cluster -host localhost -port 8900"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster test-cluster -host localhost -port 8901"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster test-cluster -host localhost -port 8902"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster test-cluster -host localhost -port 8903"));
    startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster test-cluster -host localhost -port 8904"));
    //startClusterManager(createArgs("-zkSvr localhost:2181 -cluster test-cluster"));
    Thread.sleep(60000);
    AssertJUnit.assertTrue(ClusterStateVerifier
        .verifyState(createArgs("-zkSvr localhost:2181 -cluster test-cluster")));
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

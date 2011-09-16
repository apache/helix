package com.linkedin.clustermanager;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkServer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.controller.ClusterManagerMain;
import com.linkedin.clustermanager.mock.storage.DummyProcess;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;

public class TestParticiptantNameCollision
{
  static int exceptionCount = 0;
  @Test
  public void testInvocation() throws Exception
  // public static void main(String[] args) throws Exception
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
    ClusterSetup setup = new ClusterSetup("localhost:" + port);
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addCluster ESPRESSO_STORAGE"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addCluster relay-cluster-12345"));
    ClusterSetup
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -addResourceGroup ESPRESSO_STORAGE db-12345 50 MasterSlave"));
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
        .processCommandLineArgs(createArgs("-zkSvr localhost:2181 -rebalance ESPRESSO_STORAGE db-12345 3"));
    
    List<Thread> tList = new ArrayList<Thread>();
    
    tList.add(startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8900")));
    tList.add(startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8901")));
    tList.add(startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8901")));
    tList.add(startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8903")));
    tList.add(startDummyProcess(createArgs("-zkSvr localhost:2181 -cluster ESPRESSO_STORAGE -host localhost -port 8903")));
    
        // Thread.currentThread().join();
    Thread.sleep(5000);
    AssertJUnit
        .assertTrue(exceptionCount == 2);
    zkServer.shutdown();
    
    for(Thread t: tList)
    {
      t.interrupt();
    }
  }

  private static Thread startDummyProcess(final String[] args) 
  {
    Thread t = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try
        {
          DummyProcess.main(args);
        } 
        catch(ClusterManagerException e)
        {
          exceptionCount++;
        }
        catch (Exception e)
        {
          e.printStackTrace();
        }
      }
    });
    t.start();
    try
    {
      Thread.currentThread().sleep(500);
    } catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return t;
  }

  private static String[] createArgs(String str)
  {
    String[] split = str.split("[ ]+");
    System.out.println(Arrays.toString(split));
    return split;
  }
}

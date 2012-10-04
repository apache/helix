package com.linkedin.helix.examples;

import org.I0Itec.zkclient.ZkServer;

import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.tools.ClusterSetup;

public class IdealStateExample
{

  public static void main(String[] args)
  {
    if (args.length < 3)
    {
      System.err.println("USAGE: IdealStateExample zkAddress clusterName idealStateMode (AUTO, AUTO_REBALANCE, or CUSTOMIZED)");
      System.exit(1);
    }

    final String zkAddr = args[0];
    final String clusterName = args[1];
    final String idealStateMode = args[2].toUpperCase();

    // add cluster {clusterName}
    ClusterSetup setupTool = new ClusterSetup(zkAddr);
    setupTool.addCluster(clusterName, true);

    // add 3 participants: "localhost:{12918, 12919, 12920}"
    for (int i = 0; i < 3; i++)
    {
      int port = 12918 + i;
      setupTool.addInstanceToCluster(clusterName, "localhost:" + port);
    }

    // add resource "TestDB" which has 4 partitions and uses MasterSlave state model
    String resourceName = "TestDB";
    setupTool.addResourceToCluster(clusterName, resourceName, 4, "MasterSlave", idealStateMode);

    // rebalance resource "TestDB" using 3 replicas
    setupTool.rebalanceStorageCluster(clusterName, resourceName, 3);

    // start helix controller
    new Thread(new Runnable()
    {

      @Override
      public void run()
      {
        try
        {
          HelixControllerMain.main(new String[] { "--zkSvr", zkAddr, "--cluster",
              clusterName });
        }
        catch (Exception e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

    }).start();

    // start 3 dummy participants
    for (int i = 0; i < 3; i++)
    {
      int port = 12918 + i;
      final String instanceName = "localhost_" + port;
      new Thread(new Runnable()
      {

        @Override
        public void run()
        {
          DummyParticipant.main(new String[] { zkAddr, clusterName, instanceName });
        }
      }).start();
    }

  }
}

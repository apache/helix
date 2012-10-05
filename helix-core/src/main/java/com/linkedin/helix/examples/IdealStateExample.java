package com.linkedin.helix.examples;

import java.io.File;

import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.tools.ClusterSetup;

public class IdealStateExample
{

  public static void main(String[] args) throws Exception
  {
    if (args.length < 3)
    {
      System.err.println("USAGE: IdealStateExample zkAddress clusterName idealStateMode (AUTO, AUTO_REBALANCE, or CUSTOMIZED) idealStateJsonFile (required for CUSTOMIZED mode)");
      System.exit(1);
    }

    final String zkAddr = args[0];
    final String clusterName = args[1];
    final String idealStateModeStr = args[2].toUpperCase();
    String idealStateJsonFile = null;
    IdealStateModeProperty idealStateMode =
        IdealStateModeProperty.valueOf(idealStateModeStr);
    if (idealStateMode == IdealStateModeProperty.CUSTOMIZED)
    {
      if (args.length < 4)
      {
        System.err.println("Missng idealStateJsonFile for CUSTOMIZED ideal state mode");
        System.exit(1);
      }
      idealStateJsonFile = args[3];
    }

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
    if (idealStateMode == IdealStateModeProperty.AUTO
        || idealStateMode == IdealStateModeProperty.AUTO_REBALANCE)
    {
      setupTool.addResourceToCluster(clusterName,
                                     resourceName,
                                     4,
                                     "MasterSlave",
                                     idealStateModeStr);

      // rebalance resource "TestDB" using 3 replicas
      setupTool.rebalanceStorageCluster(clusterName, resourceName, 3);
    }
    else if (idealStateMode == IdealStateModeProperty.CUSTOMIZED)
    {
      setupTool.addIdealState(clusterName, resourceName, idealStateJsonFile);
    }

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

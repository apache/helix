package com.linkedin.helix.examples;

import java.io.File;

import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.tools.ClusterSetup;
import com.linkedin.helix.tools.StateModelConfigGenerator;

/**
 * Ideal state json format file used in this example for CUSTOMIZED ideal state mode
 * <p>
 * <pre>
 * {
 * "id" : "TestDB",
 * "mapFields" : {
 *   "TestDB_0" : {
 *     "localhost_12918" : "MASTER",
 *     "localhost_12919" : "SLAVE",
 *     "localhost_12920" : "SLAVE"
 *   },
 *   "TestDB_1" : {
 *     "localhost_12918" : "MASTER",
 *     "localhost_12919" : "SLAVE",
 *     "localhost_12920" : "SLAVE"
 *   },
 *   "TestDB_2" : {
 *     "localhost_12918" : "MASTER",
 *     "localhost_12919" : "SLAVE",
 *     "localhost_12920" : "SLAVE"
 *   },
 *   "TestDB_3" : {
 *     "localhost_12918" : "MASTER",
 *     "localhost_12919" : "SLAVE",
 *     "localhost_12920" : "SLAVE"
 *   }
 * },
 * "listFields" : {
 * },
 * "simpleFields" : {
 *   "IDEAL_STATE_MODE" : "CUSTOMIZED",
 *   "NUM_PARTITIONS" : "4",
 *   "REPLICAS" : "3",
 *   "STATE_MODEL_DEF_REF" : "MasterSlave",
 *   "STATE_MODEL_FACTORY_NAME" : "DEFAULT"
 * }
 * }
 * </pre>
 * 
 */

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
    ZkClient zkclient =
        new ZkClient(zkAddr,
                     ZkClient.DEFAULT_SESSION_TIMEOUT,
                     ZkClient.DEFAULT_CONNECTION_TIMEOUT,
                     new ZNRecordSerializer());
    ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);
    admin.addCluster(clusterName, true);

    // add MasterSlave state mode definition
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    admin.addStateModelDef(clusterName,
                           "MasterSlave",
                           new StateModelDefinition(generator.generateConfigForMasterSlave()));

    // add 3 participants: "localhost:{12918, 12919, 12920}"
    for (int i = 0; i < 3; i++)
    {
      int port = 12918 + i;
      InstanceConfig config = new InstanceConfig("localhost_" + port);
      config.setHostName("localhost");
      config.setPort(Integer.toString(port));
      config.setInstanceEnabled(true);
      admin.addInstance(clusterName, config);
    }

    // add resource "TestDB" which has 4 partitions and uses MasterSlave state model
    String resourceName = "TestDB";
    if (idealStateMode == IdealStateModeProperty.AUTO
        || idealStateMode == IdealStateModeProperty.AUTO_REBALANCE)
    {
      admin.addResource(clusterName, resourceName, 4, "MasterSlave", idealStateModeStr);

      // rebalance resource "TestDB" using 3 replicas
      admin.rebalance(clusterName, resourceName, 3);
    }
    else if (idealStateMode == IdealStateModeProperty.CUSTOMIZED)
    {
      admin.addIdealState(clusterName, resourceName, idealStateJsonFile);
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

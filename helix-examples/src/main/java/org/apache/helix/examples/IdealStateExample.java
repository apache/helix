package org.apache.helix.examples;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;

/**
 * Ideal state json format file used in this example for CUSTOMIZED ideal state mode
 * <p>
 * 
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
 */

public class IdealStateExample {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err
          .println("USAGE: IdealStateExample zkAddress clusterName idealStateMode (FULL_AUTO, SEMI_AUTO, or CUSTOMIZED) idealStateJsonFile (required for CUSTOMIZED mode)");
      System.exit(1);
    }

    final String zkAddr = args[0];
    final String clusterName = args[1];
    final String idealStateRebalancerModeStr = args[2].toUpperCase();
    String idealStateJsonFile = null;
    RebalanceMode idealStateRebalancerMode = RebalanceMode.valueOf(idealStateRebalancerModeStr);
    if (idealStateRebalancerMode == RebalanceMode.CUSTOMIZED) {
      if (args.length < 4) {
        System.err.println("Missng idealStateJsonFile for CUSTOMIZED ideal state mode");
        System.exit(1);
      }
      idealStateJsonFile = args[3];
    }

    // add cluster {clusterName}
    ZkClient zkclient =
        new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            new ZNRecordSerializer());
    ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);
    admin.addCluster(clusterName, true);

    // add MasterSlave state mode definition
    admin.addStateModelDef(clusterName, "MasterSlave", new StateModelDefinition(
        StateModelConfigGenerator.generateConfigForMasterSlave()));

    // add 3 participants: "localhost:{12918, 12919, 12920}"
    for (int i = 0; i < 3; i++) {
      int port = 12918 + i;
      InstanceConfig config = new InstanceConfig("localhost_" + port);
      config.setHostName("localhost");
      config.setPort(Integer.toString(port));
      config.setInstanceEnabled(true);
      admin.addInstance(clusterName, config);
    }

    // add resource "TestDB" which has 4 partitions and uses MasterSlave state model
    String resourceName = "TestDB";
    if (idealStateRebalancerMode == RebalanceMode.SEMI_AUTO
        || idealStateRebalancerMode == RebalanceMode.FULL_AUTO) {
      admin.addResource(clusterName, resourceName, 4, "MasterSlave", idealStateRebalancerModeStr);

      // rebalance resource "TestDB" using 3 replicas
      admin.rebalance(clusterName, resourceName, 3);
    } else if (idealStateRebalancerMode == RebalanceMode.CUSTOMIZED) {
      admin.addIdealState(clusterName, resourceName, idealStateJsonFile);
    }

    // start helix controller
    new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          HelixControllerMain.main(new String[] {
              "--zkSvr", zkAddr, "--cluster", clusterName
          });
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

    }).start();

    // start 3 dummy participants
    for (int i = 0; i < 3; i++) {
      int port = 12918 + i;
      final String instanceName = "localhost_" + port;
      new Thread(new Runnable() {

        @Override
        public void run() {
          DummyParticipant.main(new String[] {
              zkAddr, clusterName, instanceName
          });
        }
      }).start();
    }

  }
}

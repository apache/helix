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
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.AutoModeISBuilder;
import org.apache.helix.model.builder.AutoRebalanceModeISBuilder;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.tools.StateModelConfigGenerator;

public class IdealStateBuilderExample {

  private static String buildPartitionName(String resourceName, int partitionNum) {
    return resourceName + "_" + partitionNum;
  }

  public static void main(String[] args) {

    if (args.length < 3) {
      System.err
          .println("USAGE: java IdealStateBuilderExample zkAddress clusterName idealStateMode"
              + " (FULL_AUTO, SEMI_AUTO, CUSTOMIZED, or USER_DEFINED)");
      System.exit(1);
    }

    final String zkAddr = args[0];
    final String clusterName = args[1];
    RebalanceMode idealStateMode = RebalanceMode.valueOf(args[2].toUpperCase());

    ZkClient zkclient =
        new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            new ZNRecordSerializer());
    ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

    // add cluster
    admin.addCluster(clusterName, true);

    // add MasterSlave state model definition
    admin.addStateModelDef(clusterName, "MasterSlave", new StateModelDefinition(
        StateModelConfigGenerator.generateConfigForMasterSlave()));

    // add 2 participants: "localhost:{12918, 12919}"
    int n = 2;
    for (int i = 0; i < n; i++) {
      int port = 12918 + i;
      InstanceConfig config = new InstanceConfig("localhost_" + port);
      config.setHostName("localhost");
      config.setPort(Integer.toString(port));
      config.setInstanceEnabled(true);
      admin.addInstance(clusterName, config);
    }

    // add ideal-state according to ideal-state-mode
    String resourceName = "TestDB";
    IdealState idealState = null;
    switch (idealStateMode) {
    case SEMI_AUTO: {
      AutoModeISBuilder builder = new AutoModeISBuilder(resourceName);
      builder.setStateModel("MasterSlave").setNumPartitions(2).setNumReplica(2);
      builder.assignPreferenceList(buildPartitionName(resourceName, 0), "localhost_12918",
          "localhost_12919").assignPreferenceList(buildPartitionName(resourceName, 1),
          "localhost_12919", "localhost_12918");

      idealState = builder.build();
      break;
    }
    case FULL_AUTO: {
      AutoRebalanceModeISBuilder builder = new AutoRebalanceModeISBuilder(resourceName);
      builder.setStateModel("MasterSlave").setNumPartitions(2).setNumReplica(2)
          .setMaxPartitionsPerNode(2);
      builder.add(buildPartitionName(resourceName, 0)).add(buildPartitionName(resourceName, 1));

      idealState = builder.build();
      break;
    }
    case CUSTOMIZED: {
      CustomModeISBuilder builder = new CustomModeISBuilder(resourceName);
      builder.setStateModel("MasterSlave").setNumPartitions(2).setNumReplica(2);
      builder
          .assignInstanceAndState(buildPartitionName(resourceName, 0), "localhost_12918", "MASTER")
          .assignInstanceAndState(buildPartitionName(resourceName, 0), "localhost_12919", "SLAVE")
          .assignInstanceAndState(buildPartitionName(resourceName, 1), "localhost_12918", "SLAVE")
          .assignInstanceAndState(buildPartitionName(resourceName, 1), "localhost_12919", "MASTER");
      idealState = builder.build();
      break;
    }
    default:
      break;
    }

    admin.addResource(clusterName, resourceName, idealState);

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

    // start dummy participants
    for (int i = 0; i < n; i++) {
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

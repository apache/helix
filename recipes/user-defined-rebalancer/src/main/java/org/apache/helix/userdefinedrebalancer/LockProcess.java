package org.apache.helix.userdefinedrebalancer;

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

import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;

public class LockProcess {
  private final String clusterName;
  private final String zkAddress;
  private final String instanceName;
  private final String stateModelName;
  private HelixManager participantManager;

  LockProcess(String clusterName, String zkAddress, String instanceName, String stateModelName) {
    this.clusterName = clusterName;
    this.zkAddress = zkAddress;
    this.instanceName = instanceName;
    this.stateModelName = stateModelName;

  }

  public void start() throws Exception {
    System.out.println("STARTING " + instanceName);
    configureInstance(instanceName);
    participantManager =
        HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
            zkAddress);
    participantManager.getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from(stateModelName), new LockFactory());
    participantManager.connect();
    System.out.println("STARTED " + instanceName);
  }

  /**
   * Configure the instance, the configuration of each node is available to
   * other nodes.
   * @param instanceName
   */
  private void configureInstance(String instanceName) {
    ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkAddress);

    List<String> instancesInCluster = helixAdmin.getInstancesInCluster(clusterName);
    if (instancesInCluster == null || !instancesInCluster.contains(instanceName)) {
      InstanceConfig config = new InstanceConfig(instanceName);
      config.setHostName("localhost");
      config.setPort("12000");
      helixAdmin.addInstance(clusterName, config);
    }
  }

  public void stop() {
    if (participantManager != null) {
      participantManager.disconnect();
    }
  }
}

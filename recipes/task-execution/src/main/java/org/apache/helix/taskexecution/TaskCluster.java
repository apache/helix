package org.apache.helix.taskexecution;

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

import org.apache.helix.ConfigAccessor;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.StateModelConfigGenerator;

public class TaskCluster {
  public static final String DEFAULT_CLUSTER_NAME = "task-cluster";
  static final String DEFAULT_STATE_MODEL = "OnlineOffline";

  ZkClient _zkclient = null;
  ZKHelixAdmin _admin = null;
  private final String _clusterName;

  public TaskCluster(String zkAddr, String clusterName) throws Exception {
    _clusterName = clusterName;
    _zkclient =
        new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT,
            new ZNRecordSerializer());
    _admin = new ZKHelixAdmin(_zkclient);
  }

  public void setup() throws Exception {
    // add cluster
    _admin.addCluster(_clusterName, true);

    // add state model definition
    // StateModelConfigGenerator generator = new StateModelConfigGenerator();
    _admin.addStateModelDef(_clusterName, DEFAULT_STATE_MODEL, new StateModelDefinition(
        StateModelConfigGenerator.generateConfigForOnlineOffline()));

  }

  public void disconnect() throws Exception {
    _zkclient.close();
  }

  public void submitDag(String dagJson) throws Exception {
    Dag dag = Dag.fromJson(dagJson);
    submitDag(dag);
  }

  public void submitDag(Dag dag) throws Exception {
    ConfigAccessor clusterConfig = new ConfigAccessor(_zkclient);
    HelixConfigScope clusterScope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(_clusterName).build();
    for (String id : dag.getNodeIds()) {
      Dag.Node node = dag.getNode(id);
      clusterConfig.set(clusterScope, node.getId(), node.toJson());
      _admin.addResource(_clusterName, node.getId(), node.getNumPartitions(), DEFAULT_STATE_MODEL,
          RebalanceMode.FULL_AUTO.toString());

    }

    for (String id : dag.getNodeIds()) {
      _admin.rebalance(_clusterName, id, 1);
    }
  }
}

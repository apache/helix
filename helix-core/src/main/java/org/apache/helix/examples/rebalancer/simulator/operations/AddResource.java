package org.apache.helix.examples.rebalancer.simulator.operations;

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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;


/**
 * Adding a new resource to the cluster.
 */
public class AddResource implements Operation {
  private final String _resourceName;
  private final String _stateModelDef;
  private final Map<String, Integer> _capacityUsage;
  private final int _partitions;
  private final int _replicas;
  private final String _rebalanceClassName;
  private final String _rebalanceStrategy;

  public AddResource(String resourceName, String stateModelDef, Map<String, Integer> capacityUsage,
      int partitions, int replicas, String rebalanceClassName, String rebalanceStrategy) {
    _resourceName = resourceName;
    _stateModelDef = stateModelDef;
    _capacityUsage = capacityUsage;
    _partitions = partitions;
    _replicas = replicas;
    _rebalanceClassName = rebalanceClassName;
    _rebalanceStrategy = rebalanceStrategy;
  }

  @Override
  public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
    admin.addResource(clusterName, _resourceName, _partitions, _stateModelDef, "AUTO");

    IdealState is = admin.getResourceIdealState(clusterName, _resourceName);
    is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    is.setRebalancerClassName(_rebalanceClassName);
    is.setRebalanceStrategy(_rebalanceStrategy);
    is.setReplicas("" + _replicas);
    admin.setResourceIdealState(clusterName, _resourceName, is);

    ResourceConfig resourceConfig = new ResourceConfig(_resourceName);
    try {
      resourceConfig.setPartitionCapacityMap(
          Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, _capacityUsage));
    } catch (IOException e) {
      throw new HelixException("Failed to set up partition capacity.", e);
    }
    new ConfigAccessor(zkClient).setResourceConfig(clusterName, _resourceName, resourceConfig);
    admin.rebalance(clusterName, _resourceName,
        is.getReplicaCount(admin.getInstancesInCluster(clusterName).size()));
    return true;
  }

  @Override
  public String getDescription() {
    return "Adding new resource: " + _resourceName;
  }
}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixAdmin;
import org.apache.helix.examples.rebalancer.simulator.AbstractSimulator;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;

/**
 * Create a new instance using the input instance config.
 */
public class AddNode implements Operation {
  private final InstanceConfig _instanceConfig;
  private final AbstractSimulator _simulator;

  public AddNode(InstanceConfig instanceConfig, AbstractSimulator simulator) {
    _instanceConfig = instanceConfig;
    _simulator = simulator;
  }

  public AddNode(String instancePrefix, int seq, Map<String, Integer> capacityMap, String zone,
      String domain, AbstractSimulator simulator) {
    _instanceConfig = getInstanceConfig(instancePrefix, seq, capacityMap, zone, domain);
    _simulator = simulator;
  }

  private InstanceConfig getInstanceConfig(String instancePrefix, int seq,
      Map<String, Integer> capacityMap, String zone, String domain) {
    int port = 12000 + seq;
    InstanceConfig instanceConfig = new InstanceConfig(instancePrefix + "_" + port);
    instanceConfig.setHostName(instancePrefix);
    instanceConfig.setPort("" + port);
    instanceConfig.setInstanceEnabled(true);
    if (domain != null) {
      instanceConfig.setDomain(domain);
    }
    instanceConfig.setInstanceCapacityMap(capacityMap);
    if (zone != null) {
      instanceConfig.setZoneId(zone);
    }
    return instanceConfig;
  }

  @Override
  public boolean execute(HelixAdmin admin, HelixZkClient zkClient, String clusterName) {
    admin.addInstance(clusterName, _instanceConfig);
    try {
      _simulator.getNodeToZoneMap()
          .put(_instanceConfig.getInstanceName(), _instanceConfig.getZoneId());
      _simulator.startNewProcess(_instanceConfig.getInstanceName(),
          Arrays.stream(BuiltInStateModelDefinitions.values())
              .map(modelDef -> modelDef.getStateModelDefinition().getId())
              .collect(Collectors.toList()));
    } catch (Exception e) {
      return false;
    }
    ZkHelixClusterVerifier verifier =
        new ZkHelixClusterVerifier(zkClient.getServers(), clusterName) {
          @Override
          public boolean verifyByZkCallback(long timeout) {
            List<ClusterVerifyTrigger> triggers = new ArrayList<>();
            triggers.add(new ClusterVerifyTrigger(_keyBuilder.liveInstances(), false, true, true));
            return verifyByCallback(timeout, triggers);
          }

          @Override
          protected boolean verifyState() {
            Set<String> actualLiveNodes =
                new HashSet<String>(_accessor.getChildNames(_keyBuilder.liveInstances()));
            return actualLiveNodes.contains(_instanceConfig.getInstanceName());
          }
        };
    return verifier.verify(5000);
  }

  @Override
  public String getDescription() {
    return "Adding new node: " + _instanceConfig.getInstanceName();
  }
}

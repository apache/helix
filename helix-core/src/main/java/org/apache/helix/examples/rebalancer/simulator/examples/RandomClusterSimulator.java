package org.apache.helix.examples.rebalancer.simulator.examples;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.examples.rebalancer.simulator.AbstractSimulator;
import org.apache.helix.examples.rebalancer.simulator.operations.AddResource;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;


/**
 * Create a random cluster based on the specified scale and run the simulated tests.
 */
public class RandomClusterSimulator extends AbstractSimulator {
  private static String ZK_ADDRESS = "localhost:2199";
  private static String CLUSTER_NAME = "RandomSimulationCluster";

  private static int INIT_NUM_NODES = 15;
  private static int NUM_RESOURCES = 10;
  private static final int NUM_PARTITIONS = 128;
  private static final int NUM_REPLICAS = 3;
  private static final int NUM_ZONES = INIT_NUM_NODES / 3;

  private String rebalanaceClass = WagedRebalancer.class.getName();
  private String rebalanceStrategy = null; //CrushEdRebalanceStrategy.class.getName();

  public RandomClusterSimulator(int transitionDelay, boolean verbose) {
    super(transitionDelay, verbose);
  }

  @Override
  protected boolean prepareMetadata() {
    // create cluster
    echo("Creating cluster: " + getClusterName());
    getAdmin().addCluster(getClusterName(), true);

    ConfigAccessor configAccessor = new ConfigAccessor(getZkClient());
    // Enable topology
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(getClusterName());
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/Zone/Instance");
    clusterConfig.setFaultZoneType("Zone");
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList("Partition"));

    configAccessor.setClusterConfig(getClusterName(), clusterConfig);

    for (BuiltInStateModelDefinitions modelDef : BuiltInStateModelDefinitions.values()) {
      StateModelDefinition def = modelDef.getStateModelDefinition();
      getAdmin().addStateModelDef(getClusterName(), def.getId(), def);
    }

    // Add node configs to the cluster
    for (int i = 0; i < INIT_NUM_NODES; i++) {
      String instancePrefix = "instance" + i;
      InstanceConfig instanceConfig = getInstanceConfig(instancePrefix, i, DEFAULT_CAPACITY);
      getAdmin().addInstance(getClusterName(), instanceConfig);
    }

    for (int ri = 0; ri < NUM_RESOURCES; ri++) {
      String resourceName = "resource" + "_" + ri;
      new AddResource(resourceName,
          BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition().getId(), DEFAULT_USAGE,
          NUM_PARTITIONS, NUM_REPLICAS, NUM_REPLICAS, rebalanaceClass, rebalanceStrategy)
          .execute(getAdmin(), getZkClient(), getClusterName());
    }
    return true;
  }

  private static InstanceConfig getInstanceConfig(String instancePrefix, int seq,
      Map<String, Integer> capacityMap) {
    int port = 12000 + seq;
    String zone = "zone-" + (seq % NUM_ZONES);
    String domain = "Zone=zone-" + (seq % NUM_ZONES) + ",Instance=" + instancePrefix;
    InstanceConfig instanceConfig = new InstanceConfig(instancePrefix + "_" + port);
    instanceConfig.setHostName(instancePrefix);
    instanceConfig.setPort("" + port);
    instanceConfig.setInstanceEnabled(true);
    instanceConfig.setDomain(domain);
    instanceConfig.setInstanceCapacityMap(capacityMap);
    instanceConfig.setZoneId(zone);
    return instanceConfig;
  }

  public static void main(String[] args)
      throws Exception {
    RandomClusterSimulator simulator = new RandomClusterSimulator(0, false);

    Set<String> faultZones = new HashSet<>();
    for (int i = 0; i < NUM_ZONES; i++) {
      faultZones.add("zone-" + i);
    }
    simulator.simulate(ZK_ADDRESS, CLUSTER_NAME, null,
        ExampleScripts.rollingUpgrade(faultZones, simulator), false);

    System.exit(0);
  }
}

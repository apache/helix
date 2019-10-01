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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.examples.rebalancer.simulator.AbstractSimulator;
import org.apache.helix.examples.rebalancer.simulator.MetadataOverwrites;
import org.apache.helix.examples.rebalancer.simulator.operations.Operation;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.tools.commandtools.ZkCopy;

/**
 * Clone the cluster metadata from a real running Helix cluster and then simulate based on that metadata.
 */
public class CloneClusterSimulator extends AbstractSimulator {
  private static String ZK_ADDRESS = "localhost:2199";
  private final String _srcZkServers;

  private Map<String, Integer> defaultCapacity = Collections.singletonMap("Partition", 1000);
  private Map<String, Integer> defaultUsage = Collections.singletonMap("Partition", 1);

  /**
   * @param srcZkServers    the source ZkServers connection string that will be cloned
   * @param transitionDelay
   * @param verbose
   */
  public CloneClusterSimulator(String srcZkServers, int transitionDelay, boolean verbose) {
    super(transitionDelay, verbose);
    _srcZkServers = srcZkServers;
  }

  @Override
  protected boolean prepareMetadata() {
    // Clean up before clone
    getAdmin().dropCluster(getClusterName());
    // Clone cluster
    echo("Cloning cluster: " + getClusterName());
    String srcURI = String.format("zk://%s/%s", _srcZkServers, getClusterName());
    String dstURI = String.format("zk://%s/%s", ZK_ADDRESS, getClusterName());
    String dstPath = String.format("/%s", getClusterName());
    try {
      ZkCopy.main(new String[] { "--src", srcURI + "/CONFIGS", "--dst", dstURI + "/CONFIGS" });
      ZkCopy
          .main(new String[] { "--src", srcURI + "/CONTROLLER", "--dst", dstURI + "/CONTROLLER" });
      ZkCopy.main(
          new String[] { "--src", srcURI + "/IDEALSTATES", "--dst", dstURI + "/IDEALSTATES" });
      ZkCopy.main(
          new String[] { "--src", srcURI + "/STATEMODELDEFS", "--dst", dstURI + "/STATEMODELDEFS"
          });
      getZkClient().createPersistent(dstPath + "/INSTANCES");
      getZkClient().createPersistent(dstPath + "/EXTERNALVIEW");
      getZkClient().createPersistent(dstPath + "/LIVEINSTANCES");
      getZkClient().createPersistent(dstPath + "/PROPERTYSTORE");
      if (getAdmin().isInMaintenanceMode(getClusterName())) {
        getAdmin()
            .manuallyEnableMaintenanceMode(getClusterName(), false, "test", Collections.emptyMap());
      }
      String instanceConfigPath = PropertyPathBuilder.instanceConfig(getClusterName());
      List<String> instanceConfigChildrenPath = getZkClient().getChildren(instanceConfigPath);
      for (String instance : instanceConfigChildrenPath) {
        getZkClient()
            .createPersistent(PropertyPathBuilder.instanceMessage(getClusterName(), instance),
                true);
        getZkClient()
            .createPersistent(PropertyPathBuilder.instanceCurrentState(getClusterName(), instance),
                true);
        getZkClient()
            .createPersistent(PropertyPathBuilder.instanceError(getClusterName(), instance), true);
        getZkClient()
            .createPersistent(PropertyPathBuilder.instanceStatusUpdate(getClusterName(), instance),
                true);
        getZkClient()
            .createPersistent(PropertyPathBuilder.instanceHistory(getClusterName(), instance),
                true);
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private MetadataOverwrites generateOverwrites() {
    return new MetadataOverwrites() {
      @Override
      public Map<String, ResourceConfig> updateResourceConfigs(
          Map<String, ResourceConfig> resourceConfigMap, Map<String, IdealState> idealStateMap) {
        Map<String, ResourceConfig> ret = new HashMap<>(resourceConfigMap);
        for (Map.Entry<String, IdealState> entry : idealStateMap.entrySet()) {
          if (entry.getValue().getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)
              || !entry.getValue().getRebalanceMode().equals(IdealState.RebalanceMode.FULL_AUTO)) {
            ret.remove(entry.getKey());
          } else {
            ResourceConfig resourceConfig = ret.get(entry.getKey());
            if (resourceConfig == null) {
              resourceConfig = new ResourceConfig(entry.getKey());
            }
            try {
              resourceConfig.setPartitionCapacityMap(
                  Collections.singletonMap(ResourceConfig.DEFAULT_PARTITION_KEY, defaultUsage));
            } catch (IOException e) {
              throw new HelixException("Failed to set usage weight.", e);
            }
            resourceConfig.setPreferenceLists(Collections.EMPTY_MAP);
          }
        }
        return ret;
      }

      @Override
      public Map<String, IdealState> updateIdealStates(Map<String, IdealState> idealStateMap) {
        Map<String, IdealState> ret = new HashMap<>(idealStateMap);
        for (Map.Entry<String, IdealState> entry : idealStateMap.entrySet()) {
          if (entry.getValue().getStateModelDefRef().equals("Task") || !entry.getValue()
              .getRebalanceMode().equals(IdealState.RebalanceMode.FULL_AUTO)) {
            ret.remove(entry.getKey());
          }
        }
        return ret;
      }

      @Override
      public Map<String, InstanceConfig> updateInstanceConfig(ClusterConfig clusterConfig,
          Map<String, InstanceConfig> instanceConfigMap) {
        for (InstanceConfig instanceConfig : instanceConfigMap.values()) {
          instanceConfig.getRecord()
              .setMapField(InstanceConfig.InstanceConfigProperty.HELIX_DISABLED_PARTITION.name(),
                  Collections.emptyMap());
          // TODO: refine the fault zone fetch logic here.
          getNodeToZoneMap().put(instanceConfig.getInstanceName(),
              instanceConfig.getDomainAsMap().get(clusterConfig.getFaultZoneType()));
        }
        return instanceConfigMap;
      }

      @Override
      public ClusterConfig updateClusterConfig(ClusterConfig clusterConfig) {
        Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preference = new HashMap<>();
        preference.put(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 3);
        preference.put(ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 7);
        clusterConfig.setGlobalRebalancePreference(preference);
        clusterConfig.setPersistIntermediateAssignment(false);
        clusterConfig.setPersistBestPossibleAssignment(true);
        clusterConfig.setInstanceCapacityKeys(Collections.singletonList("Partition"));
        clusterConfig.setDefaultInstanceCapacityMap(defaultCapacity);
        clusterConfig.getRecord().setListField(
            ClusterConfig.ClusterConfigProperty.STATE_TRANSITION_THROTTLE_CONFIGS.name(),
            Collections.emptyList());
        return clusterConfig;
      }
    };
  }

  public static void main(String[] args) throws Exception {
    String srcZkServers = "zk-ltx1-espresso.stg.linkedin.com:12913";
    String clusterName = "ESPRESSO_MT1";
    if (args.length >= 2) {
      srcZkServers = args[0];
      clusterName = args[1];
    }
    CloneClusterSimulator simulator = new CloneClusterSimulator(srcZkServers, 0, false);

    Set<String> faultZones = new HashSet<>();
    String faultZoneType;
    String topology;

    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer()).setConnectInitTimeout(3 * 1000);
    HelixZkClient srcClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(srcZkServers), clientConfig);
    try {
      ConfigAccessor configAccessor = new ConfigAccessor(srcClient);
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
      faultZoneType = clusterConfig.getFaultZoneType();
      topology = clusterConfig.getTopology();
      ZKHelixAdmin admin = new ZKHelixAdmin(srcClient);
      for (String instance : admin.getInstancesInCluster(clusterName)) {
        String faultZone =
            admin.getInstanceConfig(clusterName, instance).getDomainAsMap().get(faultZoneType);
        faultZones.add(faultZone);
      }
    } finally {
      srcClient.close();
    }

    List<Operation> operations = new ArrayList<>();
    operations.addAll(ExampleScripts.migrateToWagedRebalancer());
    operations.addAll(ExampleScripts
        .expandFaultZones(2, simulator.defaultCapacity, faultZones, topology, faultZoneType,
            simulator));
    operations.addAll(ExampleScripts.shrinkFaultZones(2, faultZones, simulator));

    simulator.simulate(ZK_ADDRESS, clusterName, simulator.generateOverwrites(), operations, false);
    System.exit(0);
  }
}

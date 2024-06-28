package org.apache.helix.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyType;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.AbstractRebalancer;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.ReadOnlyWagedRebalancer;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class HelixUtil {
  static private Logger LOG = LoggerFactory.getLogger(HelixUtil.class);

  private HelixUtil() {
  }

  public static String getInstanceNameFromPath(String path) {
    // path structure
    // /<cluster_name>/instances/<instance_name>/[currentStates/messages]
    if (path.contains("/" + PropertyType.INSTANCES + "/")) {
      String[] split = path.split("\\/");
      if (split.length > 3) {
        return split[3];
      }
    }
    return null;
  }

  /**
   * get the parent-path of given path
   * return "/" string if path = "/xxx", null if path = "/"
   * @param path
   * @return
   */
  public static String getZkParentPath(String path) {
    if (path.equals("/")) {
      return null;
    }

    int idx = path.lastIndexOf('/');
    return idx == 0 ? "/" : path.substring(0, idx);
  }

  /**
   * get the last part of the zk-path
   * @param path
   * @return
   */
  public static String getZkName(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  /**
   * Convert a cluster name to a sharding key for routing purpose by adding a "/" to the front.
   * Check if the cluster name already has a "/" at the front; if so just return it.
   * @param clusterName - cluster name
   * @return the sharding key corresponding the cluster name
   */
  public static String clusterNameToShardingKey(String clusterName) {
    return clusterName.charAt(0) == '/' ? clusterName : "/" + clusterName;
  }

  public static String serializeByComma(List<String> objects) {
    return Joiner.on(",").join(objects);
  }

  public static List<String> deserializeByComma(String object) {
    if (object.length() == 0) {
      return Collections.EMPTY_LIST;
    }
    return Arrays.asList(object.split(","));
  }

  /**
   * parse a csv-formated key-value pairs
   * @param keyValuePairs : csv-formatted key-value pairs. e.g. k1=v1,k2=v2,...
   * @return
   */
  public static Map<String, String> parseCsvFormatedKeyValuePairs(String keyValuePairs) {
    String[] pairs = keyValuePairs.split("[\\s,]");
    Map<String, String> keyValueMap = new TreeMap<String, String>();
    for (String pair : pairs) {
      int idx = pair.indexOf('=');
      if (idx == -1) {
        LOG.error("Invalid key-value pair: " + pair + ". Igonore it.");
        continue;
      }

      String key = pair.substring(0, idx);
      String value = pair.substring(idx + 1);
      keyValueMap.put(key, value);
    }
    return keyValueMap;
  }

  /**
   * Attempts to load the class and delegates to TCCL if class is not found.
   * Note: The approach is used as a last resort for environments like OSGi.
   * @param className
   * @return
   * @throws ClassNotFoundException
   */
  public static <T> Class<?> loadClass(Class<T> clazz, String className)
      throws ClassNotFoundException {
    try {
      return clazz.getClassLoader().loadClass(className);
    } catch (ClassNotFoundException ex) {
      if (Thread.currentThread().getContextClassLoader() != null) {
        return Thread.currentThread().getContextClassLoader().loadClass(className);
      } else {
        throw ex;
      }
    }
  }

  /**
   * Returns the expected ideal ResourceAssignments for the given resources in the cluster
   * calculated using the read-only WAGED rebalancer. The returned result is based on preference
   * lists, which is the target stable assignment.
   * @param metadataStoreAddress
   * @param clusterConfig
   * @param instanceConfigs
   * @param liveInstances
   * @param idealStates
   * @param resourceConfigs
   * @return
   */
  public static Map<String, ResourceAssignment> getTargetAssignmentForWagedFullAuto(
      String metadataStoreAddress, ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs, List<String> liveInstances,
      List<IdealState> idealStates, List<ResourceConfig> resourceConfigs) {
    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(metadataStoreAddress);
    Map<String, ResourceAssignment> result =
        getAssignmentForWagedFullAutoImpl(new ZkBucketDataAccessor(metadataStoreAddress),
            baseDataAccessor, clusterConfig, instanceConfigs, liveInstances, idealStates,
            resourceConfigs, false);
    baseDataAccessor.close();
    return result;
  }

  /**
   * Returns the expected ideal ResourceAssignments for the given resources in the cluster
   * calculated using the read-only WAGED rebalancer. The returned result is based on preference
   * lists, which is the target stable assignment.
   * @param zkBucketDataAccessor
   * @param baseDataAccessor
   * @param clusterConfig
   * @param instanceConfigs
   * @param liveInstances
   * @param idealStates
   * @param resourceConfigs
   * @return
   */
  public static Map<String, ResourceAssignment> getTargetAssignmentForWagedFullAuto(
      ZkBucketDataAccessor zkBucketDataAccessor, BaseDataAccessor<ZNRecord> baseDataAccessor,
      ClusterConfig clusterConfig, List<InstanceConfig> instanceConfigs, List<String> liveInstances,
      List<IdealState> idealStates, List<ResourceConfig> resourceConfigs) {
    return getAssignmentForWagedFullAutoImpl(zkBucketDataAccessor, baseDataAccessor, clusterConfig,
        instanceConfigs, liveInstances, idealStates, resourceConfigs, true);
  }

  /**
   * Returns the expected ideal ResourceAssignments for the given resources in the cluster
   * calculated using the read-only WAGED rebalancer. The returned result is based on partition
   * state mapping. which is the immediate assignment. The immediate assignment is different from
   * the final target assignment; it could be an intermediate state where it contains replicas that
   * need to be dropped later, for example.
   * @param metadataStoreAddress
   * @param clusterConfig
   * @param instanceConfigs
   * @param liveInstances
   * @param idealStates
   * @param resourceConfigs
   * @return
   */
  public static Map<String, ResourceAssignment> getImmediateAssignmentForWagedFullAuto(
      String metadataStoreAddress, ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs, List<String> liveInstances,
      List<IdealState> idealStates, List<ResourceConfig> resourceConfigs) {
    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(metadataStoreAddress);
    Map<String, ResourceAssignment> result =
        getAssignmentForWagedFullAutoImpl(new ZkBucketDataAccessor(metadataStoreAddress),
            baseDataAccessor, clusterConfig, instanceConfigs, liveInstances, idealStates,
            resourceConfigs, false);
    baseDataAccessor.close();
    return result;
  }

  /*
   * If usePrefLists is set to true, the returned assignment is based on preference lists; if
   * false, the returned assignment is based on partition state mapping, which may differ from
   * preference lists.
   */
  private static Map<String, ResourceAssignment> getAssignmentForWagedFullAutoImpl(
      ZkBucketDataAccessor zkBucketDataAccessor, BaseDataAccessor<ZNRecord> baseDataAccessor,
      ClusterConfig clusterConfig, List<InstanceConfig> instanceConfigs, List<String> liveInstances,
      List<IdealState> idealStates, List<ResourceConfig> resourceConfigs, boolean usePrefLists) {

    // Copy the cluster config and make globalRebalance happen synchronously
    // Otherwise, globalRebalance may not complete and this util might end up returning
    // an empty assignment.
    ClusterConfig globalSyncClusterConfig = new ClusterConfig(clusterConfig.getRecord());
    globalSyncClusterConfig.setGlobalRebalanceAsyncMode(false);

    // Prepare a data accessor for a dataProvider (cache) refresh
    HelixDataAccessor helixDataAccessor =
        new ZKHelixDataAccessor(globalSyncClusterConfig.getClusterName(), baseDataAccessor);

    // Create an instance of read-only WAGED rebalancer
    ReadOnlyWagedRebalancer readOnlyWagedRebalancer =
        new ReadOnlyWagedRebalancer(zkBucketDataAccessor, globalSyncClusterConfig.getClusterName(),
            globalSyncClusterConfig.getGlobalRebalancePreference());

    // Use a dummy event to run the required stages for BestPossibleState calculation
    // Attributes RESOURCES and RESOURCES_TO_REBALANCE are populated in ResourceComputationStage
    ClusterEvent event =
        new ClusterEvent(globalSyncClusterConfig.getClusterName(), ClusterEventType.Unknown);

    try {
      // First, prepare waged rebalancer with a snapshot, so that it can react on the difference
      // between the current snapshot and the provided parameters which act as the new snapshot
      ResourceControllerDataProvider dataProvider =
          new ResourceControllerDataProvider(globalSyncClusterConfig.getClusterName());
      dataProvider.requireFullRefresh();
      dataProvider.refresh(helixDataAccessor);
      readOnlyWagedRebalancer.updateChangeDetectorSnapshots(dataProvider);
      // Refresh dataProvider completely to populate _refreshedChangeTypes
      dataProvider.requireFullRefresh();
      dataProvider.refresh(helixDataAccessor);

      dataProvider.setClusterConfig(globalSyncClusterConfig);
      dataProvider.setInstanceConfigMap(instanceConfigs.stream()
          .collect(Collectors.toMap(InstanceConfig::getInstanceName, Function.identity())));
      // For LiveInstances, we must preserve the existing session IDs
      // So read LiveInstance objects from the cluster and do a "retainAll" on them
      // assignableLiveInstanceMap is an unmodifiableMap instances, so we filter using a stream
      Map<String, LiveInstance> assignableLiveInstanceMap = dataProvider.getAssignableLiveInstances();
      List<LiveInstance> filteredLiveInstances = assignableLiveInstanceMap.entrySet().stream()
          .filter(entry -> liveInstances.contains(entry.getKey())).map(Map.Entry::getValue)
          .collect(Collectors.toList());
      // Synthetically create LiveInstance objects that are passed in as the parameter
      // First, determine which new LiveInstance objects need to be created
      List<String> liveInstanceList = new ArrayList<>(liveInstances);
      liveInstanceList.removeAll(filteredLiveInstances.stream().map(LiveInstance::getInstanceName)
          .collect(Collectors.toList()));
      liveInstanceList.forEach(liveInstanceName -> {
        // Create a new LiveInstance object and give it a random UUID as a session ID
        LiveInstance newLiveInstanceObj = new LiveInstance(liveInstanceName);
        newLiveInstanceObj.getRecord()
            .setSimpleField(LiveInstance.LiveInstanceProperty.SESSION_ID.name(),
                UUID.randomUUID().toString().replace("-", ""));
        filteredLiveInstances.add(newLiveInstanceObj);
      });
      dataProvider.setLiveInstances(new ArrayList<>(filteredLiveInstances));
      dataProvider.setIdealStates(idealStates);
      dataProvider.setResourceConfigMap(resourceConfigs.stream()
          .collect(Collectors.toMap(ResourceConfig::getResourceName, Function.identity())));

      event.addAttribute(AttributeName.ControllerDataProvider.name(), dataProvider);
      event.addAttribute(AttributeName.STATEFUL_REBALANCER.name(), readOnlyWagedRebalancer);

      // Run the required stages to obtain the BestPossibleOutput
      RebalanceUtil.runStage(event, new ResourceComputationStage());
      RebalanceUtil.runStage(event, new CurrentStateComputationStage());
      RebalanceUtil.runStage(event, new BestPossibleStateCalcStage());
    } catch (Exception e) {
      LOG.error("getIdealAssignmentForWagedFullAuto(): Failed to compute ResourceAssignments!", e);
    } finally {
      // Close all ZK connections
      readOnlyWagedRebalancer.close();
    }

    // Convert the resulting BestPossibleStateOutput to Map<String, ResourceAssignment>
    Map<String, ResourceAssignment> result = new HashMap<>();
    BestPossibleStateOutput output = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    if (output == null || (output.getPreferenceLists() == null && output.getResourceStatesMap()
        .isEmpty())) {
      throw new HelixException(
          "getIdealAssignmentForWagedFullAuto(): Calculation failed: Failed to compute BestPossibleState!");
    }
    for (IdealState idealState : idealStates) {
      String resourceName = idealState.getResourceName();
      StateModelDefinition stateModelDefinition =
          BuiltInStateModelDefinitions.valueOf(idealState.getStateModelDefRef())
              .getStateModelDefinition();
      PartitionStateMap partitionStateMap = output.getPartitionStateMap(resourceName);
      ResourceAssignment resourceAssignment = new ResourceAssignment(resourceName);
      for (String partitionName : idealState.getPartitionSet()) {
        Partition partition = new Partition(partitionName);
        if (usePrefLists) {
          resourceAssignment.addReplicaMap(partition,
              computeIdealMapping(output.getPreferenceList(resourceName, partitionName),
                  stateModelDefinition, new HashSet<>(liveInstances)));
        } else {
          resourceAssignment.addReplicaMap(partition, partitionStateMap.getPartitionMap(partition));
        }
      }
      result.put(resourceName, resourceAssignment);
    }
    return result;
  }

  /**
   * This method provides the ideal state mapping with corresponding rebalance strategy
   * @param clusterConfig         The cluster config
   * @param instanceConfigs       List of all existing instance configs including disabled/down instances
   * @param liveInstances         List of live and enabled instance names
   * @param idealState            The ideal state of current resource. If input is null, will be
   *                              treated as newly created resource.
   * @param partitions            The list of partition names
   * @param strategyClassName          The rebalance strategy. e.g. AutoRebalanceStrategy
   * @return A map of ideal state assignment as partition -> instance -> state
   */
  public static Map<String, Map<String, String>> getIdealAssignmentForFullAuto(
      ClusterConfig clusterConfig, List<InstanceConfig> instanceConfigs, List<String> liveInstances,
      IdealState idealState, List<String> partitions, String strategyClassName)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    List<String> allNodes = new ArrayList<>();
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      allNodes.add(instanceConfig.getInstanceName());
      instanceConfigMap.put(instanceConfig.getInstanceName(), instanceConfig);
    }
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider();
    cache.setClusterConfig(clusterConfig);
    cache.setInstanceConfigMap(instanceConfigMap);

    StateModelDefinition stateModelDefinition =
        BuiltInStateModelDefinitions.valueOf(idealState.getStateModelDefRef())
            .getStateModelDefinition();

    RebalanceStrategy strategy =
        RebalanceStrategy.class.cast(loadClass(HelixUtil.class, strategyClassName).newInstance());

    strategy.init(idealState.getResourceName(), partitions, stateModelDefinition
            .getStateCountMap(liveInstances.size(), idealState.getReplicaCount(liveInstances.size())),
        idealState.getMaxPartitionsPerInstance());

    // Remove all disabled instances so that Helix will not consider them live.
    List<String> disabledInstance = instanceConfigs.stream()
        .filter(instanceConfig -> !instanceConfig.getInstanceEnabled())
        .map(InstanceConfig::getInstanceName)
        .collect(Collectors.toList());
    liveInstances.removeAll(disabledInstance);

    Map<String, List<String>> preferenceLists = strategy
        .computePartitionAssignment(allNodes, liveInstances,
            new HashMap<String, Map<String, String>>(), cache).getListFields();

    Map<String, Map<String, String>> idealStateMapping = new HashMap<>();
    Set<String> liveInstanceSet = new HashSet<>(liveInstances);
    for (String partitionName : preferenceLists.keySet()) {
      idealStateMapping.put(partitionName,
          computeIdealMapping(preferenceLists.get(partitionName), stateModelDefinition,
              liveInstanceSet));
    }
    return idealStateMapping;
  }

  /**
   * compute the ideal mapping for resource in Full-Auto and Semi-Auto based on its preference list
   */
  public static Map<String, String> computeIdealMapping(List<String> preferenceList,
      StateModelDefinition stateModelDef, Set<String> liveAndEnabled) {
    return computeIdealMapping(preferenceList, stateModelDef, liveAndEnabled,
        Collections.emptySet());
  }

  /**
   * compute the ideal mapping for resource in Full-Auto and Semi-Auto based on its preference list
   */
  public static Map<String, String> computeIdealMapping(List<String> preferenceList,
      StateModelDefinition stateModelDef, Set<String> liveInstanceSet,
      Set<String> disabledInstancesForPartition) {
    Map<String, String> idealStateMap = new HashMap<String, String>();

    if (preferenceList == null) {
      return idealStateMap;
    }

    for (String instance : preferenceList) {
      if (disabledInstancesForPartition.contains(instance) && liveInstanceSet.contains(instance)) {
        idealStateMap.put(instance, stateModelDef.getInitialState());
      }
    }

    Set<String> liveAndEnabledInstances = new HashSet<>(liveInstanceSet);
    liveAndEnabledInstances.removeAll(disabledInstancesForPartition);

    List<String> statesPriorityList = stateModelDef.getStatesPriorityList();
    Set<String> assigned = new HashSet<String>();

    for (String state : statesPriorityList) {
      int stateCount = AbstractRebalancer
          .getStateCount(state, stateModelDef, liveAndEnabledInstances.size(), preferenceList.size());
      for (String instance : preferenceList) {
        if (stateCount <= 0) {
          break;
        }
        if (!assigned.contains(instance) && liveAndEnabledInstances.contains(instance)) {
          idealStateMap.put(instance, state);
          assigned.add(instance);
          stateCount--;
        }
      }
    }

    return idealStateMap;
  }

  /**
   * Remove the given message from ZK using the given accessor. This function will
   * not throw exception
   * @param accessor HelixDataAccessor
   * @param msg message to remove
   * @param instanceName name of the instance on which the message sits
   * @return true if success else false
   */
  public static boolean removeMessageFromZK(HelixDataAccessor accessor, Message msg,
      String instanceName) {
    try {
      return accessor.removeProperty(msg.getKey(accessor.keyBuilder(), instanceName));
    } catch (Exception e) {
      LOG.error("Caught exception while removing message {}.", msg, e);
    }
    return false;
  }

  /**
   * Get the value of system property
   * @param propertyKey
   * @param propertyDefaultValue
   * @return
   */
  public static int getSystemPropertyAsInt(String propertyKey, int propertyDefaultValue) {
    String valueString = System.getProperty(propertyKey, "" + propertyDefaultValue);

    try {
      int value = Integer.parseInt(valueString);
      if (value > 0) {
        return value;
      }
    } catch (NumberFormatException e) {
      LOG.warn("Exception while parsing property: " + propertyKey + ", string: " + valueString
          + ", using default value: " + propertyDefaultValue);
    }

    return propertyDefaultValue;
  }

  /**
   * Get the value of system property
   * @param propertyKey
   * @param propertyDefaultValue
   * @return
   */
  public static long getSystemPropertyAsLong(String propertyKey, long propertyDefaultValue) {
    String valueString = System.getProperty(propertyKey, "" + propertyDefaultValue);

    try {
      long value = Long.parseLong(valueString);
      if (value > 0) {
        return value;
      }
    } catch (NumberFormatException e) {
      LOG.warn("Exception while parsing property: " + propertyKey + ", string: " + valueString
          + ", using default value: " + propertyDefaultValue);
    }

    return propertyDefaultValue;
  }

  /**
   * Compose the config for an instance
   * @param instanceName the unique name of the instance
   * @return InstanceConfig
   */
  public static InstanceConfig composeInstanceConfig(String instanceName) {
    return new InstanceConfig.Builder().build(instanceName);
  }

  /**
   * Checks whether or not the cluster is in management mode. It checks:
   * - pause signal
   * - live instances: whether any live instance is not in normal status, eg. frozen.
   * - messages: whether live instance has a participant status change message
   *
   * @param pauseSignal pause signal
   * @param liveInstanceMap map of live instances
   * @param enabledLiveInstances set of enabled live instance names. They should be all included
   *                             in the liveInstanceMap.
   * @param instancesMessages a map of all instances' messages.
   * @return true if cluster is in management mode; otherwise, false
   */
  public static boolean inManagementMode(PauseSignal pauseSignal,
      Map<String, LiveInstance> liveInstanceMap, Set<String> enabledLiveInstances,
      Map<String, Collection<Message>> instancesMessages) {
    // Check pause signal and abnormal live instances (eg. in freeze mode)
    // TODO: should check maintenance signal when moving maintenance to management pipeline
    return pauseSignal != null || enabledLiveInstances.stream().anyMatch(
        instance -> isInstanceInManagementMode(instance, liveInstanceMap, instancesMessages));
  }

  private static boolean isInstanceInManagementMode(String instance,
      Map<String, LiveInstance> liveInstanceMap,
      Map<String, Collection<Message>> instancesMessages) {
    // Check live instance status and participant status change message
    return LiveInstance.LiveInstanceStatus.FROZEN.equals(liveInstanceMap.get(instance).getStatus())
        || (instancesMessages.getOrDefault(instance, Collections.emptyList()).stream()
        .anyMatch(Message::isParticipantStatusChangeType));
  }

  /**
   * Sort zoneMapping for each virtual group and flatten to a list.
   * @param zoneMapping virtual group mapping.
   * @return a list of instances sorted and flattened.
   */
  public static List<String> sortAndFlattenZoneMapping(Map<String, Set<String>> zoneMapping) {
    return zoneMapping
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByKey())
        .flatMap(entry -> entry.getValue().stream().sorted())
        .collect(Collectors.toList());
  }
}

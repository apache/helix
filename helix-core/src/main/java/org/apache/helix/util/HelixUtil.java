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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyType;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.AbstractRebalancer;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
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
            .getStateCountMap(liveInstances.size(), Integer.parseInt(idealState.getReplicas())),
        idealState.getMaxPartitionsPerInstance());

    // Remove all disabled instances so that Helix will not consider them live.
    List<String> disabledInstance =
        instanceConfigs.stream().filter(enabled -> !enabled.getInstanceEnabled())
            .map(InstanceConfig::getInstanceName).collect(Collectors.toList());
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

}

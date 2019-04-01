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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.task.TaskConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for validating Helix properties
 * Warning: each method validates one single property of instance individually and independently.
 * One validation wouldn't depend on the results of other validations
 * TODO: add unit tests
 */
public class InstanceValidationUtil {
  private static final Logger _logger = LoggerFactory.getLogger(InstanceValidationUtil.class);

  public enum HealthStatusType {
    instanceHealthStatus,
    partitionHealthStatus
  }

  private InstanceValidationUtil() {
  }

  /**
   * Method to check if the instance is enabled by configuration
   * @param dataAccessor
   * @param configAccessor
   * @param clusterId
   * @param instanceName
   * @return
   */
  public static boolean isEnabled(HelixDataAccessor dataAccessor, ConfigAccessor configAccessor,
      String clusterId, String instanceName) {
    // TODO use static builder instance to reduce GC
    PropertyKey propertyKey = new PropertyKey.Builder(clusterId).instanceConfig(instanceName);
    InstanceConfig instanceConfig = dataAccessor.getProperty(propertyKey);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterId);
    // TODO deprecate instance level config checks once migrated the enable status to cluster config
    // only
    boolean enabledInInstanceConfig = instanceConfig != null && instanceConfig.getInstanceEnabled();

    if (clusterConfig != null) {
      Map<String, String> disabledInstances = clusterConfig.getDisabledInstances();
      boolean enabledInClusterConfig =
          disabledInstances == null || !disabledInstances.keySet().contains(instanceName);
      return enabledInClusterConfig && enabledInInstanceConfig;
    }

    return false;
  }

  /**
   * Method to check if the instance is up and running by configuration
   * @param dataAccessor
   * @param clusterId
   * @param instanceName
   * @return
   */
  public static boolean isAlive(HelixDataAccessor dataAccessor, String clusterId,
      String instanceName) {
    PropertyKey propertyKey = new PropertyKey.Builder(clusterId).liveInstance(instanceName);

    return dataAccessor.getBaseDataAccessor().exists(propertyKey.getPath(),
        AccessOption.PERSISTENT);
  }

  /**
   * Method to check if the instance is assigned at least 1 resource, not in a idle state;
   * Independent of the instance alive/enabled status
   * @param dataAccessor
   * @param clusterId
   * @param instanceName
   * @return
   */
  public static boolean hasResourceAssigned(HelixDataAccessor dataAccessor, String clusterId,
      String instanceName) {
    PropertyKey.Builder propertyKeyBuilder = new PropertyKey.Builder(clusterId);
    PropertyKey liveInstanceKey = propertyKeyBuilder.liveInstance(instanceName);
    LiveInstance liveInstance = dataAccessor.getProperty(liveInstanceKey);
    if (liveInstance != null) {
      String sessionId = liveInstance.getSessionId();

      PropertyKey currentStatesKey =
          propertyKeyBuilder.currentStates(instanceName, sessionId);
      List<String> resourceNames = dataAccessor.getChildNames(currentStatesKey);
      for (String resourceName : resourceNames) {
        PropertyKey key =
            propertyKeyBuilder.currentState(instanceName, sessionId, resourceName);
        CurrentState currentState = dataAccessor.getProperty(key);
        if (currentState != null && currentState.getPartitionStateMap().size() > 0) {
          return true;
        }
      }
    }

    _logger.warn(String.format("The instance %s is not active", instanceName));
    return false;
  }

  /**
   * Method to check if the instance has any disabled partition assigned
   * @param dataAccessor
   * @param clusterId
   * @param instanceName
   * @return
   */
  public static boolean hasDisabledPartitions(HelixDataAccessor dataAccessor, String clusterId,
      String instanceName) {
    PropertyKey propertyKey = new PropertyKey.Builder(clusterId).instanceConfig(instanceName);
    InstanceConfig instanceConfig = dataAccessor.getProperty(propertyKey);
    if (instanceConfig != null) {
      return !instanceConfig.getDisabledPartitionsMap().isEmpty();
    }

    throw new HelixException("Fail to get instance config for " + instanceName);
  }

  /**
   * Method to check if the instance has valid configuration
   * @param dataAccessor
   * @param clusterId
   * @param instanceName
   * @return
   */
  public static boolean hasValidConfig(HelixDataAccessor dataAccessor, String clusterId,
      String instanceName) {
    PropertyKey propertyKey = new PropertyKey.Builder(clusterId).instanceConfig(instanceName);
    InstanceConfig instanceConfig = dataAccessor.getProperty(propertyKey);
    return instanceConfig != null && instanceConfig.isValid();
  }

  /**
   * Method to check if the instance has error partitions
   * @param dataAccessor
   * @param clusterId
   * @param instanceName
   * @return
   */
  public static boolean hasErrorPartitions(HelixDataAccessor dataAccessor, String clusterId,
      String instanceName) {
    PropertyKey.Builder propertyKeyBuilder = new PropertyKey.Builder(clusterId);
    PropertyKey liveInstanceKey = propertyKeyBuilder.liveInstance(instanceName);
    LiveInstance liveInstance = dataAccessor.getProperty(liveInstanceKey);
    if (liveInstance != null) {
      String sessionId = liveInstance.getSessionId();

      PropertyKey currentStatesKey =
          propertyKeyBuilder.currentStates(instanceName, sessionId);
      List<String> resourceNames = dataAccessor.getChildNames(currentStatesKey);
      for (String resourceName : resourceNames) {
        PropertyKey key =
            propertyKeyBuilder.currentState(instanceName, sessionId, resourceName);

        CurrentState currentState = dataAccessor.getProperty(key);
        if (currentState != null
            && currentState.getPartitionStateMap().containsValue(HelixDefinedState.ERROR.name())) {
          return true;
        }
      }
    }

    _logger.warn(String.format("The instance %s is not active", instanceName));
    return false;
  }

  /**
   * Check the overall health status for instance including:
   *  1. Per instance health status with application customized key-value entries
   *  2. Sibling partitions (replicas for same partition holding on different node
   *     health status for the entire cluster.
   *
   * @param configAccessor
   * @param clustername
   * @param hostName
   * @param customizedInputs
   * @param partitionHealthMap
   * @return
   */
  public static boolean checkCustomizedHealthStatusForInstance(ConfigAccessor configAccessor,
      String clustername, String hostName, Map<String, String> customizedInputs,
      Map<String, Map<String, String>> partitionHealthMap, Map<String, String> instanceHealthMap) {
    boolean isHealthy = true;
    RESTConfig restConfig = configAccessor.getRESTConfig(clustername);
    // If user customized URL is not ready, return true as the check
    if (restConfig == null || restConfig.getCustomizedHealthURL() == null) {
      return isHealthy;
    }
    // TODO : 1. Call REST with customized URL
    //        2. Parse mapping result with string -> boolean value and return out for per instance
    //        3. Check sibling nodes for partition health
    isHealthy =
        perInstanceHealthCheck(instanceHealthMap) || perPartitionHealthCheck(partitionHealthMap);

    return isHealthy;
  }

  /**
   * Fetch the health map based on health type: per instance or per partition
   * Accessor can used for fetching data from ZK for per partition level.
   * @param URL
   * @param accessor
   * @param healthStatusType
   * @return
   */
  public static Map<String, Map<String, String>> getHealthMapBasedOnType(String URL,
      HelixDataAccessor accessor, HealthStatusType healthStatusType) {
    return null;
  }

  protected static boolean perInstanceHealthCheck(Map<String, String> statusMap) {
    return true;
  }

  protected static boolean perPartitionHealthCheck(
      Map<String, Map<String, String>> partitionHealthMap) {
    return true;
  }

  /**
   * Check instance is already in the stable state. Here stable means all the ideal state mapping
   * matches external view (view of current state).
   *
   * It requires persist assignment on!
   *
   * @param dataAccessor
   * @param instanceName
   * @return
   */
  public static boolean isInstanceStable(HelixDataAccessor dataAccessor, String instanceName) {
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    ClusterConfig clusterConfig = dataAccessor.getProperty(keyBuilder.clusterConfig());
    if (!clusterConfig.isPersistIntermediateAssignment()) {
      throw new HelixException("isInstanceStable needs persist assignment on!");
    }

    List<String> idealStateNames = dataAccessor.getChildNames(keyBuilder.idealStates());
    for (String idealStateName : idealStateNames) {
      IdealState idealState = dataAccessor.getProperty(keyBuilder.idealStates(idealStateName));
      if (idealState == null || !idealState.isEnabled() || !idealState.isValid()
          || TaskConstants.STATE_MODEL_NAME.equals(idealState.getStateModelDefRef())) {
        continue;
      }

      ExternalView externalView = dataAccessor.getProperty(keyBuilder.externalView(idealStateName));
      if (externalView == null) {
        throw new HelixException(
            String.format("Resource %s does not have external view!", idealStateName));
      }
      for (String partition : idealState.getPartitionSet()) {
        Map<String, String> isPartitionMap = idealState.getInstanceStateMap(partition);
        Map<String, String> evPartitionMap = externalView.getStateMap(partition);
        if (isPartitionMap.containsKey(instanceName) && (!evPartitionMap.containsKey(instanceName)
            || !evPartitionMap.get(instanceName).equals(isPartitionMap.get(instanceName)))) {
          // only checks the state from IS matches EV. Return false when
          // 1. This partition not has current state on this instance
          // 2. The state does not match the state on ideal state
          return false;
        }
      }
    }
    return true;
  }
}

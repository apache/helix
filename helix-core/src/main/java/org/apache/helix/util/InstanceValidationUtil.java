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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

/**
 * Utility class for validating Helix properties
 * Warning: each method validates one single property of instance individually and independently.
 * One validation wouldn't depend on the results of other validations
 */
public class InstanceValidationUtil {
  private static final Logger _logger = LoggerFactory.getLogger(InstanceValidationUtil.class);

  public static Set<String> UNHEALTHY_STATES =
      ImmutableSet.of(HelixDefinedState.DROPPED.name(), HelixDefinedState.ERROR.name());

  private InstanceValidationUtil() {
  }

  /**
   * Method to check if the instance is enabled by configuration
   * @param dataAccessor
   * @param instanceName
   * @return
   */
  public static boolean isEnabled(HelixDataAccessor dataAccessor, String instanceName) {
    PropertyKey.Builder propertyKeyBuilder = dataAccessor.keyBuilder();
    InstanceConfig instanceConfig = dataAccessor.getProperty(propertyKeyBuilder.instanceConfig(instanceName));
    ClusterConfig clusterConfig = dataAccessor.getProperty(propertyKeyBuilder.clusterConfig());
    // TODO deprecate instance level config checks once migrated the enable status to cluster config only
    if (instanceConfig == null || clusterConfig == null) {
      throw new HelixException("InstanceConfig or ClusterConfig is NULL");
    }

    boolean enabledInInstanceConfig = instanceConfig.getInstanceEnabled();
    Map<String, String> disabledInstances = clusterConfig.getDisabledInstances();
    boolean enabledInClusterConfig =
        disabledInstances == null || !disabledInstances.keySet().contains(instanceName);
    return enabledInClusterConfig && enabledInInstanceConfig;
  }

  /**
   * Method to check if the instance is up and running by configuration
   * @param dataAccessor
   * @param instanceName
   * @return
   */
  public static boolean isAlive(HelixDataAccessor dataAccessor, String instanceName) {
    LiveInstance liveInstance = dataAccessor.getProperty(dataAccessor.keyBuilder().liveInstance(instanceName));
    return liveInstance != null;
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
    PropertyKey.Builder propertyKeyBuilder = dataAccessor.keyBuilder();
    LiveInstance liveInstance = dataAccessor.getProperty(propertyKeyBuilder.liveInstance(instanceName));
    if (liveInstance != null) {
      String sessionId = liveInstance.getEphemeralOwner();

      List<String> resourceNames = dataAccessor.getChildNames(propertyKeyBuilder.currentStates(instanceName, sessionId));
      for (String resourceName : resourceNames) {
        PropertyKey currentStateKey = propertyKeyBuilder.currentState(instanceName, sessionId, resourceName);
        CurrentState currentState = dataAccessor.getProperty(currentStateKey);
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
    PropertyKey propertyKey = dataAccessor.keyBuilder().instanceConfig(instanceName);
    InstanceConfig instanceConfig = dataAccessor.getProperty(propertyKey);
    if (instanceConfig != null) {
      // Originally, Helix only checks whether disabled partition map has entries or not. But this
      // could cause problem when some of partitions disabled and enabled back, the resource entries
      // are still left there. For detailed check, we shall check the whether partition list is empty
      // or not
      for (List<String> disabledPartitions : instanceConfig.getDisabledPartitionsMap().values()) {
        if (disabledPartitions != null && disabledPartitions.size() > 0) {
          return true;
        }
      }
      return false;
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
    PropertyKey propertyKey = dataAccessor.keyBuilder().instanceConfig(instanceName);
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
      String sessionId = liveInstance.getEphemeralOwner();

      PropertyKey currentStatesKey = propertyKeyBuilder.currentStates(instanceName, sessionId);
      List<String> resourceNames = dataAccessor.getChildNames(currentStatesKey);
      for (String resourceName : resourceNames) {
        PropertyKey key = propertyKeyBuilder.currentState(instanceName, sessionId, resourceName);

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
   * Get the problematic partitions on the to-be-stop instance
   * Requirement:
   *  If the instance gets stopped and the partitions on the instance are OFFLINE,
   *  the cluster still have enough "healthy" replicas on other sibling instances
   *
   *  - sibling instances mean those who share the same partition (replicas) of the to-be-stop instance
   *
   * @param globalPartitionHealthStatus (instance => (partition name, health status))
   * @param instanceToBeStop The instance to be stopped
   * @param dataAccessor The data accessor
   * @return A list of problematic partitions if the instance is stopped
   */
  public static List<String> perPartitionHealthCheck(List<ExternalView> externalViews,
      Map<String, Map<String, Boolean>> globalPartitionHealthStatus, String instanceToBeStop,
      HelixDataAccessor dataAccessor) {
    List<String> unhealthyPartitions = new ArrayList<>();

    for (ExternalView externalView : externalViews) {
      // Skip ANY_LIVEINSTANCES resources check, since ANY_LIVEINSTANCES resources have single partition
      // with 1 replica. There is no need to check sibiling replicas.
      if (ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name()
          .equals(externalView.getReplicas())) {
        continue;
      }

      StateModelDefinition stateModelDefinition = dataAccessor
          .getProperty(dataAccessor.keyBuilder().stateModelDef(externalView.getStateModelDefRef()));
      for (String partition : externalView.getPartitionSet()) {
        Map<String, String> stateMap = externalView.getStateMap(partition);
        // Only check if instance holds top state
        if (stateMap.containsKey(instanceToBeStop)
            && stateMap.get(instanceToBeStop).equals(stateModelDefinition.getTopState())) {
          for (String siblingInstance : stateMap.keySet()) {
            // Skip this self check
            if (siblingInstance.equals(instanceToBeStop)) {
              continue;
            }

            // We are checking sibling partition healthy status. So if partition health does not
            // exist or it is not healthy. We should mark this partition is unhealthy.
            if (!globalPartitionHealthStatus.containsKey(siblingInstance)
                || !globalPartitionHealthStatus.get(siblingInstance).containsKey(partition)
                || !globalPartitionHealthStatus.get(siblingInstance).get(partition)) {
              unhealthyPartitions.add(partition);
              break;
            }
          }
        }
      }
    }

    return unhealthyPartitions;
  }

  /**
   * Check instance is already in the stable state. Here stable means all the ideal state mapping
   * matches external view (view of current state).
   * It requires PERSIST_INTERMEDIATE_ASSIGNMENT turned on!
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

  /**
   * Check if sibling nodes of the instance meet min active replicas constraint
   * Two instances are sibling of each other if they host the same partition
   * WARNING: The check uses ExternalView to reduce network traffic but suffer from accuracy
   * due to external view propagation latency
   *
   * TODO: Use in memory cache and query instance's currentStates
   *
   * @param dataAccessor
   * @param instanceName
   * @return
   */
  public static boolean siblingNodesActiveReplicaCheck(HelixDataAccessor dataAccessor, String instanceName) {
    PropertyKey.Builder propertyKeyBuilder = dataAccessor.keyBuilder();
    List<String> resources = dataAccessor.getChildNames(propertyKeyBuilder.idealStates());

    for (String resourceName : resources) {
      IdealState idealState = dataAccessor.getProperty(propertyKeyBuilder.idealStates(resourceName));
      if (idealState == null || !idealState.isEnabled() || !idealState.isValid()
          || TaskConstants.STATE_MODEL_NAME.equals(idealState.getStateModelDefRef())) {
        continue;
      }
      ExternalView externalView =
          dataAccessor.getProperty(propertyKeyBuilder.externalView(resourceName));
      if (externalView == null) {
        throw new HelixException(
            String.format("Resource %s does not have external view!", resourceName));
      }
      // Get the minActiveReplicas constraint for the resource
      int minActiveReplicas = externalView.getMinActiveReplicas();
      if (minActiveReplicas == -1) {
        _logger.warn("Resource " + resourceName
            + " is missing minActiveReplica field. Skip the sibling check");
        continue;
      }
      String stateModeDef = externalView.getStateModelDefRef();
      StateModelDefinition stateModelDefinition =
          dataAccessor.getProperty(propertyKeyBuilder.stateModelDef(stateModeDef));
      Set<String> unhealthyStates = new HashSet<>(UNHEALTHY_STATES);
      if (stateModelDefinition != null) {
        unhealthyStates.add(stateModelDefinition.getInitialState());
      }
      for (String partition : externalView.getPartitionSet()) {
        Map<String, String> stateByInstanceMap = externalView.getStateMap(partition);
        // found the resource hosted on the instance
        if (stateByInstanceMap.containsKey(instanceName)) {
          int numHealthySiblings = 0;
          for (Map.Entry<String, String> entry : stateByInstanceMap.entrySet()) {
            if (!entry.getKey().equals(instanceName)
                && !unhealthyStates.contains(entry.getValue())) {
              numHealthySiblings++;
            }
          }
          if (numHealthySiblings < minActiveReplicas) {
            _logger.info(
                "Partition {} doesn't have enough active replicas in sibling nodes. NumHealthySiblings: {}, minActiveReplicas: {}",
                partition, numHealthySiblings, minActiveReplicas);
            return false;
          }
        }
      }
    }

    return true;
  }
}

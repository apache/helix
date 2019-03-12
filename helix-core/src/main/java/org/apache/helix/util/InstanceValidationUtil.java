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
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
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
}

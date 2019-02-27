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
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;


/**
 * Utility class for validating Helix properties
 * Warning: each method validates one single property of instance individually and independently.
 * One validation wouldn't depend on the results of other validations
 * TODO: integrate on-demand cache if the performance is the bottleneck
 * TODO: manually tested the function, need to add detailed integration test
 */
public class InstanceValidationUtil {
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
    PropertyKey liveInstanceKey = new PropertyKey.Builder(clusterId).liveInstance(instanceName);
    LiveInstance liveInstance = dataAccessor.getProperty(liveInstanceKey);
    if (liveInstance != null) {
      String sessionId = liveInstance.getSessionId();

      PropertyKey currentStatesKey =
          new PropertyKey.Builder(clusterId).currentStates(instanceName, sessionId);
      List<String> resourceNames = dataAccessor.getChildNames(currentStatesKey);
      for (String resourceName : resourceNames) {
        PropertyKey key =
            dataAccessor.keyBuilder().currentState(instanceName, sessionId, resourceName);
        CurrentState currentState = dataAccessor.getProperty(key);
        if (currentState.getPartitionStateMap().size() > 0) {
          return true;
        }
      }
    }

    return false;
  }
}

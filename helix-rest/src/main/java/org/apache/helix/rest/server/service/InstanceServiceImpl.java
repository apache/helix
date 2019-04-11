package org.apache.helix.rest.server.service;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import java.util.stream.Collectors;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.client.CustomRestClientFactory;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstanceServiceImpl implements InstanceService {
  private static final Logger _logger = LoggerFactory.getLogger(InstanceServiceImpl.class);

  private final HelixDataAccessor _dataAccessor;
  private final ConfigAccessor _configAccessor;

  public InstanceServiceImpl(HelixDataAccessor dataAccessor, ConfigAccessor configAccessor) {
    _dataAccessor = dataAccessor;
    _configAccessor = configAccessor;
  }

  @Override
  public Map<String, Boolean> getInstanceHealthStatus(String clusterId, String instanceName,
      List<HealthCheck> healthChecks) {
    Map<String, Boolean> healthStatus = new TreeMap<>();
    for (HealthCheck healthCheck : healthChecks) {
      switch (healthCheck) {
      case INVALID_CONFIG:
        healthStatus.put(HealthCheck.INVALID_CONFIG.name(),
            InstanceValidationUtil.hasValidConfig(_dataAccessor, clusterId, instanceName));
        if (!healthStatus.get(HealthCheck.INVALID_CONFIG.name())) {
          _logger.error("The instance {} doesn't have valid configuration", instanceName);
          return healthStatus;
        }
      case INSTANCE_NOT_ENABLED:
        healthStatus.put(HealthCheck.INSTANCE_NOT_ENABLED.name(), InstanceValidationUtil
            .isEnabled(_dataAccessor, _configAccessor, clusterId, instanceName));
        break;
      case INSTANCE_NOT_ALIVE:
        healthStatus.put(HealthCheck.INSTANCE_NOT_ALIVE.name(),
            InstanceValidationUtil.isAlive(_dataAccessor, clusterId, instanceName));
        break;
      case INSTANCE_NOT_STABLE:
        boolean isStable = InstanceValidationUtil.isInstanceStable(_dataAccessor, instanceName);
        healthStatus.put(HealthCheck.INSTANCE_NOT_STABLE.name(), isStable);
        break;
      case HAS_ERROR_PARTITION:
        healthStatus.put(HealthCheck.HAS_ERROR_PARTITION.name(),
            !InstanceValidationUtil.hasErrorPartitions(_dataAccessor, clusterId, instanceName));
        break;
      case HAS_DISABLED_PARTITION:
        healthStatus.put(HealthCheck.HAS_DISABLED_PARTITION.name(),
            !InstanceValidationUtil.hasDisabledPartitions(_dataAccessor, clusterId, instanceName));
        break;
      case EMPTY_RESOURCE_ASSIGNMENT:
        healthStatus.put(HealthCheck.EMPTY_RESOURCE_ASSIGNMENT.name(),
            InstanceValidationUtil.hasResourceAssigned(_dataAccessor, clusterId, instanceName));
        break;
      case MIN_ACTIVE_REPLICA_CHECK_FAILED:
        healthStatus.put(HealthCheck.MIN_ACTIVE_REPLICA_CHECK_FAILED.name(),
            InstanceValidationUtil.siblingNodesActiveReplicaCheck(_dataAccessor, instanceName));
        break;
      default:
        _logger.error("Unsupported health check: {}", healthCheck);
        break;
      }
    }

    return healthStatus;
  }

  @Override
  public InstanceInfo getInstanceInfo(String clusterId, String instanceName,
      List<HealthCheck> healthChecks) {
    InstanceInfo.Builder instanceInfoBuilder = new InstanceInfo.Builder(instanceName);

    InstanceConfig instanceConfig =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().instanceConfig(instanceName));
    LiveInstance liveInstance =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().liveInstance(instanceName));
    if (instanceConfig != null) {
      instanceInfoBuilder.instanceConfig(instanceConfig.getRecord());
    }
    if (liveInstance != null) {
      instanceInfoBuilder.liveInstance(liveInstance.getRecord());
      String sessionId = liveInstance.getSessionId();

      List<String> resourceNames = _dataAccessor
          .getChildNames(_dataAccessor.keyBuilder().currentStates(instanceName, sessionId));
      instanceInfoBuilder.resources(resourceNames);
      List<String> partitions = new ArrayList<>();
      for (String resourceName : resourceNames) {
        CurrentState currentState = _dataAccessor.getProperty(
            _dataAccessor.keyBuilder().currentState(instanceName, sessionId, resourceName));
        if (currentState != null && currentState.getPartitionStateMap() != null) {
          partitions.addAll(currentState.getPartitionStateMap().keySet());
        }
      }
      instanceInfoBuilder.partitions(partitions);
    }
    try {
      Map<String, Boolean> healthStatus = getInstanceHealthStatus(clusterId, instanceName, healthChecks);
      instanceInfoBuilder.healthStatus(healthStatus);
    } catch (HelixException ex) {
      _logger.error("Exception while getting health status: {}, reporting health status as unHealth", ex);
      instanceInfoBuilder.healthStatus(false);
    }

    return instanceInfoBuilder.build();
  }


  @Override
  public StoppableCheck checkSingleInstanceStoppable(String clusterId, String instanceName,
      String jsonContent) {
    // TODO reduce GC by dependency injection
    Map<String, Boolean> helixStoppableCheck = getInstanceHealthStatus(clusterId,
        instanceName, InstanceService.HealthCheck.STOPPABLE_CHECK_LIST);
    CustomRestClient customClient = CustomRestClientFactory.get(jsonContent);
    // TODO add the json content parse logic
    Map<String, Boolean> customStoppableCheck =
        customClient.getInstanceStoppableCheck(Collections.<String, String> emptyMap());
    StoppableCheck stoppableCheck =
        StoppableCheck.mergeStoppableChecks(helixStoppableCheck, customStoppableCheck);
    return stoppableCheck;
  }

  /**
   * Perform customized single instance health check map filtering
   *
   * Input map is user customized health out put. It will be HEALTH_ENTRY_KEY -> true/false
   * @param statusMap
   * @return
   */
  private Map<String, Boolean> perInstanceHealthCheck(Map<String, Boolean> statusMap) {
    if (statusMap != null && !statusMap.isEmpty()) {
      statusMap = statusMap.entrySet().stream().filter(entry -> !entry.getValue())
          .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));
    }
    return statusMap;
  }
}

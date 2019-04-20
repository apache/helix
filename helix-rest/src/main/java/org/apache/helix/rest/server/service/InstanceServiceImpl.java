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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.client.CustomRestClientFactory;
import org.apache.helix.rest.server.json.cluster.PartitionHealth;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstanceServiceImpl implements InstanceService {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceServiceImpl.class);

  private static final String PARTITION_HEALTH_KEY = "PARTITION_HEALTH";
  private static final String IS_HEALTHY_KEY = "IS_HEALTHY";
  private static final String EXPIRY_KEY = "EXPIRE";

  private final HelixDataAccessor _dataAccessor;
  private final ConfigAccessor _configAccessor;

  public InstanceServiceImpl(HelixDataAccessor dataAccessor, ConfigAccessor configAccessor) {
    _dataAccessor = dataAccessor;
    _configAccessor = configAccessor;
  }

  @Override
  public Map<String, Boolean> getInstanceHealthStatus(String clusterId, String instanceName,
      List<HealthCheck> healthChecks) {
    Map<String, Boolean> healthStatus = new HashMap<>();
    for (HealthCheck healthCheck : healthChecks) {
      switch (healthCheck) {
      case INVALID_CONFIG:
        healthStatus.put(HealthCheck.INVALID_CONFIG.name(),
            InstanceValidationUtil.hasValidConfig(_dataAccessor, clusterId, instanceName));
        if (!healthStatus.get(HealthCheck.INVALID_CONFIG.name())) {
          LOG.error("The instance {} doesn't have valid configuration", instanceName);
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
        LOG.error("Unsupported health check: {}", healthCheck);
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
      LOG.error("Exception while getting health status: {}, reporting health status as unHealth", ex);
      instanceInfoBuilder.healthStatus(false);
    }

    return instanceInfoBuilder.build();
  }


  @Override
  public StoppableCheck checkSingleInstanceStoppable(String clusterId, String instanceName,
      String jsonContent) throws IOException {
    // TODO reduce GC by dependency injection
    Map<String, Boolean> helixStoppableCheck = getInstanceHealthStatus(clusterId,
        instanceName, InstanceService.HealthCheck.STOPPABLE_CHECK_LIST);
    CustomRestClient customClient = CustomRestClientFactory.get(jsonContent);
    try {
      Map<String, Boolean> customStoppableCheck =
        customClient.getInstanceStoppableCheck("", Collections.emptyMap());
      return StoppableCheck.mergeStoppableChecks(helixStoppableCheck, customStoppableCheck);
    } catch (IOException e) {
      LOG.error("Failed to perform customized health check for {}/{}", clusterId, instanceName, e);
      throw e;
    }
  }

  public PartitionHealth generatePartitionHealthMapFromZK() {
    PartitionHealth partitionHealth = new PartitionHealth();

    // Only checks the instances are online with valid reports
    List<String> liveInstances =
        _dataAccessor.getChildNames(_dataAccessor.keyBuilder().liveInstances());
    for (String instance : liveInstances) {
      ZNRecord customizedHealth = _dataAccessor
          .getProperty(_dataAccessor.keyBuilder().healthReport(instance, PARTITION_HEALTH_KEY))
          .getRecord();
      for (String partitionName : customizedHealth.getMapFields().keySet()) {
        try {
          Map<String, String> healthMap = customizedHealth.getMapField(partitionName);
          if (healthMap == null || Long.parseLong(healthMap.get(EXPIRY_KEY)) < System
              .currentTimeMillis()) {
            // Clean all the existing checks. If we do not clean it, when we do the customized check,
            // Helix may think these partitions are only partitions holding on the instance.
            // But it could potentially have some partitions are unhealthy for expired ones.
            // It could problem for shutting down instances.
            partitionHealth.addInstanceThatNeedDirectCallWithPartition(instance, partitionName);
            continue;
          }

          partitionHealth.addSinglePartitionHealthForInstance(instance, partitionName,
              Boolean.valueOf(healthMap.get(IS_HEALTHY_KEY)));
        } catch (Exception e) {
          LOG.warn(
              "Error in processing partition level health for instance {}, partition {}, directly querying API",
              instance, partitionName, e);
          partitionHealth.addInstanceThatNeedDirectCallWithPartition(instance, partitionName);
        }
      }
    }

    return partitionHealth;
  }

  /**
   * Get general customized URL from RESTConfig
   *
   * @param configAccessor
   * @param clustername
   *
   * @return null if RESTConfig is null
   */
  protected String getGeneralCustomizedURL(ConfigAccessor configAccessor, String clustername) {
    RESTConfig restConfig = configAccessor.getRESTConfig(clustername);
    // If user customized URL is not ready, return true as the check
    if (restConfig == null) {
      return null;
    }
    return restConfig.getCustomizedHealthURL();
  }

  /**
   * Use get user provided general URL to construct the stoppable status or partition status URL
   *
   * @param generalURL
   * @param instanceName
   * @param statusType
   * @return null if URL is malformed
   */
  protected String getCustomizedURLWithEndPoint(String generalURL, String instanceName,
      InstanceValidationUtil.HealthStatusType statusType) {
    if (generalURL == null) {
      LOG.warn("Failed to generate customized URL for instance {}", instanceName);
      return null;
    }

    try {
      // If user customized URL is not ready, return true as the check
      String hostName = instanceName.split("_")[0];
      return String.format("%s/%s", generalURL.replace("*", hostName), statusType.name());
    } catch (Exception e) {
      LOG.info("Failed to prepare customized check for generalURL {} instance {}", generalURL,
          instanceName, e);
      return null;
    }
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

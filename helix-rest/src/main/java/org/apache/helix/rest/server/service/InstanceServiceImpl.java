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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.client.CustomRestClientFactory;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;
import org.apache.helix.rest.server.json.cluster.PartitionHealth;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

public class InstanceServiceImpl implements InstanceService {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceServiceImpl.class);

  private static final String PARTITION_HEALTH_KEY = "PARTITION_HEALTH";
  private static final String IS_HEALTHY_KEY = "IS_HEALTHY";
  private static final String EXPIRY_KEY = "EXPIRE";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ExecutorService POOL = Executors.newCachedThreadPool();

  private final HelixDataAccessorWrapper _dataAccessor;
  private final ConfigAccessor _configAccessor;
  private final CustomRestClient _customRestClient;

  public InstanceServiceImpl(HelixDataAccessorWrapper dataAccessor, ConfigAccessor configAccessor) {
    _dataAccessor = dataAccessor;
    _configAccessor = configAccessor;
    _customRestClient = CustomRestClientFactory.get();
  }

  @VisibleForTesting
  InstanceServiceImpl(HelixDataAccessorWrapper dataAccessor, ConfigAccessor configAccessor,
      CustomRestClient customRestClient) {
    _dataAccessor = dataAccessor;
    _configAccessor = configAccessor;
    _customRestClient = customRestClient;
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
      String sessionId = liveInstance.getEphemeralOwner();

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
      Map<String, Boolean> healthStatus =
          getInstanceHealthStatus(clusterId, instanceName, healthChecks);
      instanceInfoBuilder.healthStatus(healthStatus);
    } catch (HelixException ex) {
      LOG.error(
          "Exception while getting health status. Cluster: {}, Instance: {}, reporting health status as unHealth",
          clusterId, instanceName, ex);
      instanceInfoBuilder.healthStatus(false);
    }

    return instanceInfoBuilder.build();
  }

  /**
   * {@inheritDoc}
   * Single instance stoppable check implementation is a special case of
   * {@link #batchGetInstancesStoppableChecks(String, List, String)}
   * <p>
   * Step 1: Perform instance level Helix own health checks
   * Step 2: Perform instance level client side health checks
   * Step 3: Perform partition level (all partitions on the instance) client side health checks
   * <p>
   * Note: if the check fails at one step, the rest steps won't be executed because the instance
   * cannot be stopped
   */
  @Override
  public StoppableCheck getInstanceStoppableCheck(String clusterId, String instanceName,
      String jsonContent) throws IOException {
    return batchGetInstancesStoppableChecks(clusterId, ImmutableList.of(instanceName), jsonContent)
        .get(instanceName);
  }

  @Override
  public Map<String, StoppableCheck> batchGetInstancesStoppableChecks(String clusterId,
      List<String> instances, String jsonContent) throws IOException {
    Map<String, StoppableCheck> finalStoppableChecks = new HashMap<>();
    Map<String, Future<StoppableCheck>> helixInstanceChecks =
        instances.stream().collect(Collectors.toMap(Function.identity(),
            instance -> POOL.submit(() -> performHelixOwnInstanceCheck(clusterId, instance))));
    List<String> instancesForCustomInstanceLevelChecks =
        filterInstancesForNextCheck(helixInstanceChecks, finalStoppableChecks);
    if (instancesForCustomInstanceLevelChecks.isEmpty()) {
      // if all instances failed at helix custom level checks
      return finalStoppableChecks;
    }

    RESTConfig restConfig = _configAccessor.getRESTConfig(clusterId);
    if (restConfig == null) {
      String errorMessage =
          String.format("The cluster %s hasn't enabled client side health checks yet, "
              + "thus the stoppable check result is inaccurate", clusterId);
      LOG.error(errorMessage);
      throw new HelixException(errorMessage);
    }
    Map<String, String> customPayLoads = getCustomPayLoads(jsonContent);
    Map<String, Future<StoppableCheck>> customInstanceLevelChecks =
        instancesForCustomInstanceLevelChecks.stream()
            .collect(Collectors.toMap(Function.identity(),
                instance -> POOL.submit(() -> performCustomInstanceCheck(clusterId, instance,
                    getBaseUrl(instance, restConfig), customPayLoads))));
    List<String> instancesForCustomPartitionLevelChecks =
        filterInstancesForNextCheck(customInstanceLevelChecks, finalStoppableChecks);
    if (!instancesForCustomPartitionLevelChecks.isEmpty()) {
      Map<String, StoppableCheck> instancePartitionLevelChecks = performPartitionsCheck(
          instancesForCustomPartitionLevelChecks, restConfig, customPayLoads);
      for (Map.Entry<String, StoppableCheck> instancePartitionStoppableCheckEntry : instancePartitionLevelChecks
          .entrySet()) {
        finalStoppableChecks.put(instancePartitionStoppableCheckEntry.getKey(),
            instancePartitionStoppableCheckEntry.getValue());
      }
    }

    return finalStoppableChecks;
  }

  private List<String> filterInstancesForNextCheck(
      Map<String, Future<StoppableCheck>> futureStoppableChecks,
      Map<String, StoppableCheck> finalStoppableChecks) {
    List<String> instancesForNextCheck = new ArrayList<>();
    for (Map.Entry<String, Future<StoppableCheck>> entry : futureStoppableChecks.entrySet()) {
      String instance = entry.getKey();
      try {
        StoppableCheck stoppableCheck = entry.getValue().get();
        if (!stoppableCheck.isStoppable()) {
          finalStoppableChecks.put(instance, stoppableCheck);
        } else {
          instancesForNextCheck.add(instance);
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Failed to get StoppableChecks in parallel. Instance: {}", instance);
      }
    }

    return instancesForNextCheck;
  }

  private StoppableCheck performHelixOwnInstanceCheck(String clusterId, String instanceName) {
    LOG.info("Perform helix own custom health checks for {}/{}", clusterId, instanceName);
    Map<String, Boolean> helixStoppableCheck = getInstanceHealthStatus(clusterId, instanceName,
        InstanceService.HealthCheck.STOPPABLE_CHECK_LIST);

    return new StoppableCheck(helixStoppableCheck, StoppableCheck.Category.HELIX_OWN_CHECK);
  }

  private Map<String, String> getCustomPayLoads(String jsonContent) throws IOException {
    Map<String, String> result = new HashMap<>();
    JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonContent);
    // parsing the inputs as string key value pairs
    jsonNode.fields().forEachRemaining(kv -> result.put(kv.getKey(), kv.getValue().asText()));
    return result;
  }

  @VisibleForTesting
  protected PartitionHealth generatePartitionHealthMapFromZK() {
    PartitionHealth partitionHealth = new PartitionHealth();

    // Only checks the instances are online with valid reports
    List<String> liveInstances =
        _dataAccessor.getChildNames(_dataAccessor.keyBuilder().liveInstances());
    // Make a parallel batch call for getting all healthreports from ZK.
    List<HelixProperty> healthReports = _dataAccessor.getProperty(liveInstances.stream()
        .map(instance -> _dataAccessor.keyBuilder().healthReport(instance, PARTITION_HEALTH_KEY))
        .collect(Collectors.toList()));
    for (int i = 0; i < liveInstances.size(); i++) {
      String instance = liveInstances.get(i);
      // TODO: Check ZNRecord is null or not. Need logic to check whether the healthreports exist
      // or not. If it does not exist, we should query the participant directly for the health
      // report.
      ZNRecord customizedHealth = healthReports.get(i).getRecord();
      for (String partitionName : customizedHealth.getMapFields().keySet()) {
        try {
          Map<String, String> healthMap = customizedHealth.getMapField(partitionName);
          if (healthMap == null
              || Long.parseLong(healthMap.get(EXPIRY_KEY)) < System.currentTimeMillis()) {
            // Clean all the existing checks. If we do not clean it, when we do the customized
            // check,
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

  private StoppableCheck performCustomInstanceCheck(String clusterId, String instanceName,
      String baseUrl, Map<String, String> customPayLoads) throws IOException {
    try {
      return new StoppableCheck(
          _customRestClient.getInstanceStoppableCheck(baseUrl, customPayLoads),
          StoppableCheck.Category.CUSTOM_INSTANCE_CHECK);
    } catch (IOException e) {
      LOG.error("Failed to perform custom client side instance level health checks for {}/{}",
          clusterId, instanceName, e);
      throw e;
    }
  }

  private Map<String, StoppableCheck> performPartitionsCheck(List<String> instances,
      RESTConfig restConfig, Map<String, String> customPayLoads) {
    PartitionHealth clusterPartitionsHealth = generatePartitionHealthMapFromZK();
    // update the health status for those expired partitions on instances
    Map<String, List<String>> expiredPartitionsByInstance =
        clusterPartitionsHealth.getExpiredRecords();
    Map<String, Future<Map<String, Boolean>>> updatedPartitionsHealthStatusByInstance =
        new HashMap<>();
    for (Map.Entry<String, List<String>> entry : expiredPartitionsByInstance.entrySet()) {
      String instance = entry.getKey();
      List<String> expiredPartitions = entry.getValue();
      Callable<Map<String, Boolean>> refreshTask =
          () -> _customRestClient.getPartitionStoppableCheck(getBaseUrl(instance, restConfig),
              expiredPartitions, customPayLoads);
      updatedPartitionsHealthStatusByInstance.put(instance, POOL.submit(refreshTask));
    }
    updatedPartitionsHealthStatusByInstance.entrySet().forEach(instanceToPartitionStatus -> {
      String instance = instanceToPartitionStatus.getKey();
      try {
        Map<String, Boolean> updatedPartitionsHealth = instanceToPartitionStatus.getValue().get();
        updatedPartitionsHealth.entrySet().forEach(
            partitionToHealthStatus -> clusterPartitionsHealth.updatePartitionHealth(instance,
                partitionToHealthStatus.getKey(), partitionToHealthStatus.getValue()));
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Failed to get partition status on instance {}", instance, e);
      }
    });
    // sibling checks on partitions health for entire cluster
    PropertyKey.Builder propertyKeyBuilder = _dataAccessor.keyBuilder();
    List<ExternalView> externalViews =
        _dataAccessor.getChildNames(propertyKeyBuilder.externalViews()).stream()
            .map(externalView -> (ExternalView) _dataAccessor
                .getProperty(propertyKeyBuilder.externalView(externalView)))
            .collect(Collectors.toList());

    Map<String, StoppableCheck> instanceStoppableChecks = new HashMap<>();
    for (String instanceName : instances) {
      List<String> unHealthyPartitions =
          InstanceValidationUtil.perPartitionHealthCheck(externalViews,
              clusterPartitionsHealth.getGlobalPartitionHealth(), instanceName, _dataAccessor);
      StoppableCheck stoppableCheck = new StoppableCheck(unHealthyPartitions.isEmpty(),
          unHealthyPartitions, StoppableCheck.Category.CUSTOM_PARTITION_CHECK);
      instanceStoppableChecks.put(instanceName, stoppableCheck);
    }

    return instanceStoppableChecks;
  }

  @VisibleForTesting
  protected Map<String, Boolean> getInstanceHealthStatus(String clusterId, String instanceName,
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
        healthStatus.put(HealthCheck.INSTANCE_NOT_ENABLED.name(),
            InstanceValidationUtil.isEnabled(_dataAccessor, instanceName));
        break;
      case INSTANCE_NOT_ALIVE:
        healthStatus.put(HealthCheck.INSTANCE_NOT_ALIVE.name(),
            InstanceValidationUtil.isAlive(_dataAccessor, instanceName));
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

  private String getBaseUrl(String instance, RESTConfig restConfig) {
    String baseUrl = restConfig.get(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL);
    // pre-assumption of the url, must be format of "http://*/path", the wildcard is replaceable by
    // the instance vip
    assert baseUrl.contains("*");
    // pre-assumption of the instance name, must be format of <instanceVip>_<port>
    assert instance.contains("_");
    String instanceVip = instance.substring(0, instance.indexOf('_'));
    return baseUrl.replace("*", instanceVip);
  }
}

package org.apache.helix.rest.clusterMaintenanceService;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.client.CustomRestClientFactory;
import org.apache.helix.rest.clusterMaintenanceService.api.OperationInterface;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;
import org.apache.helix.rest.common.datamodel.RestSnapShot;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.rest.server.resources.helix.PerInstanceAccessor;
import org.apache.helix.rest.server.service.InstanceService;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MaintenanceManagementService {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceManagementService.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ExecutorService POOL = Executors.newCachedThreadPool();

  // Metric names for custom instance check
  private static final String CUSTOM_INSTANCE_CHECK_HTTP_REQUESTS_ERROR_TOTAL =
      MetricRegistry.name(InstanceService.class, "custom_instance_check_http_requests_error_total");
  private static final String CUSTOM_INSTANCE_CHECK_HTTP_REQUESTS_DURATION =
      MetricRegistry.name(InstanceService.class, "custom_instance_check_http_requests_duration");
  public static final String ALL_HEALTH_CHECK_NONBLOCK = "allHealthCheckNonBlock";
  public static final String HELIX_INSTANCE_STOPPABLE_CHECK = "HelixInstanceStoppableCheck";
  public static final String HELIX_CUSTOM_STOPPABLE_CHECK = "CustomInstanceStoppableCheck";
  public static final String OPERATION_CONFIG_SHARED_INPUT = "OperationConfigSharedInput";

  public static final Set<StoppableCheck.Category> SKIPPABLE_HEALTH_CHECK_CATEGORIES =
      ImmutableSet.of(StoppableCheck.Category.CUSTOM_INSTANCE_CHECK,
          StoppableCheck.Category.CUSTOM_PARTITION_CHECK,
          StoppableCheck.Category.CUSTOM_AGGREGATED_CHECK);

  private final ConfigAccessor _configAccessor;
  private final CustomRestClient _customRestClient;
  private final String _namespace;
  private final boolean _skipZKRead;
  private final HelixDataAccessorWrapper _dataAccessor;
  private final Set<String> _nonBlockingHealthChecks;
  private final Set<StoppableCheck.Category> _skipHealthCheckCategories;
  // Set the default value of _skipStoppableHealthCheckList to be an empty list to
  // maintain the backward compatibility with users who don't use MaintenanceManagementServiceBuilder
  // to create the MaintenanceManagementService object.
  private List<HealthCheck> _skipStoppableHealthCheckList = Collections.emptyList();
  // default value false to maintain backward compatibility
  private boolean _skipCustomChecksIfNoLiveness = false;

  public MaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
      ConfigAccessor configAccessor, boolean skipZKRead, String namespace) {
    this(new HelixDataAccessorWrapper(dataAccessor, CustomRestClientFactory.get(), namespace),
        configAccessor, CustomRestClientFactory.get(), skipZKRead, Collections.emptySet(),
        Collections.emptySet(), namespace);
  }

  public MaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
      ConfigAccessor configAccessor, boolean skipZKRead, Set<String> nonBlockingHealthChecks,
      String namespace) {
    this(new HelixDataAccessorWrapper(dataAccessor, CustomRestClientFactory.get(), namespace),
        configAccessor, CustomRestClientFactory.get(), skipZKRead, nonBlockingHealthChecks,
        Collections.emptySet(), namespace);
  }

  public MaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
      ConfigAccessor configAccessor, boolean skipZKRead, boolean continueOnFailure,
      String namespace) {
    this(new HelixDataAccessorWrapper(dataAccessor, CustomRestClientFactory.get(), namespace),
        configAccessor, CustomRestClientFactory.get(), skipZKRead,
        continueOnFailure ? Collections.singleton(ALL_HEALTH_CHECK_NONBLOCK)
            : Collections.emptySet(), Collections.emptySet(), namespace);
  }

  public MaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
      ConfigAccessor configAccessor, boolean skipZKRead, boolean continueOnFailure,
      Set<StoppableCheck.Category> skipHealthCheckCategories, String namespace) {
    this(new HelixDataAccessorWrapper(dataAccessor, CustomRestClientFactory.get(), namespace),
        configAccessor, CustomRestClientFactory.get(), skipZKRead,
        continueOnFailure ? Collections.singleton(ALL_HEALTH_CHECK_NONBLOCK)
            : Collections.emptySet(),
        skipHealthCheckCategories != null ? skipHealthCheckCategories : Collections.emptySet(),
        namespace);
  }

  @VisibleForTesting
  MaintenanceManagementService(HelixDataAccessorWrapper dataAccessorWrapper,
      ConfigAccessor configAccessor, CustomRestClient customRestClient, boolean skipZKRead,
      Set<String> nonBlockingHealthChecks, Set<StoppableCheck.Category> skipHealthCheckCategories,
      String namespace) {
    _dataAccessor = dataAccessorWrapper;
    _configAccessor = configAccessor;
    _customRestClient = customRestClient;
    _skipZKRead = skipZKRead;
    _nonBlockingHealthChecks = nonBlockingHealthChecks;
    _skipHealthCheckCategories =
        skipHealthCheckCategories != null ? skipHealthCheckCategories : Collections.emptySet();
    _namespace = namespace;
  }

  private MaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
      ConfigAccessor configAccessor, CustomRestClient customRestClient, boolean skipZKRead,
      Set<String> nonBlockingHealthChecks, Set<StoppableCheck.Category> skipHealthCheckCategories,
      List<HealthCheck> skipStoppableHealthCheckList, String namespace, boolean skipCustomChecksIfNoLiveness) {
    _dataAccessor =
        new HelixDataAccessorWrapper(dataAccessor, customRestClient,
            namespace);
    _configAccessor = configAccessor;
    _customRestClient = customRestClient;
    _skipZKRead = skipZKRead;
    _nonBlockingHealthChecks =
        nonBlockingHealthChecks == null ? Collections.emptySet() : nonBlockingHealthChecks;
    _skipHealthCheckCategories =
        skipHealthCheckCategories == null ? Collections.emptySet() : skipHealthCheckCategories;
    _skipStoppableHealthCheckList = skipStoppableHealthCheckList == null ? Collections.emptyList()
            : skipStoppableHealthCheckList;
    _namespace = namespace;
    _skipCustomChecksIfNoLiveness = skipCustomChecksIfNoLiveness;
  }

  /**
   * Perform health check and maintenance operation check and execution for a instance in
   * one cluster.
   * User need to implement OperationAbstractClass for customer operation check & execution.
   * It will invoke OperationAbstractClass.operationCheckForTakeSingleInstance and
   * OperationAbstractClass.operationExecForTakeSingleInstance.
   * The list of check and operation will be executed in the user provided sequence.
   *
   * @param clusterId          The cluster id
   * @param instanceName       The instance name
   * @param healthChecks       A list of healthChecks to perform
   * @param healthCheckConfig The input for health Checks
   * @param operations         A list of operation checks or operations to execute
   * @param operationConfig    A map of config. Key is the operation name value if a Json
   *                            representation of a map
   * @param performOperation   If this param is set to false, the function will only do a dry run
   * @return MaintenanceManagementInstanceInfo
   * @throws IOException in case of network failure
   */
  public MaintenanceManagementInstanceInfo takeInstance(String clusterId, String instanceName,
      List<String> healthChecks, Map<String, String> healthCheckConfig, List<String> operations,
      Map<String, String> operationConfig, boolean performOperation) throws IOException {

    if ((healthChecks == null || healthChecks.isEmpty()) && (operations == null || operations
        .isEmpty())) {
      MaintenanceManagementInstanceInfo result = new MaintenanceManagementInstanceInfo(
          MaintenanceManagementInstanceInfo.OperationalStatus.FAILURE);
      result.addMessage("Invalid input. Please provide at least one health check or operation.");
      return result;
    }

    return takeFreeSingleInstanceHelper(clusterId, instanceName, healthChecks, healthCheckConfig,
        operations, operationConfig, performOperation, true);
  }

  /**
   * Perform health check and maintenance operation check and execution for a list of instances in
   * one cluster.
   * User need to implement OperationAbstractClass for customer operation check & execution.
   * It will invoke OperationAbstractClass.operationCheckForTakeInstances and
   * OperationAbstractClass.operationExecForTakeInstances.
   * The list of check and operation will be executed in the user provided sequence.
   *
   * @param clusterId          The cluster id
   * @param instances          A list of instances
   * @param healthChecks       A list of healthChecks to perform
   * @param healthCheckConfig The input for health Checks
   * @param operations         A list of operation checks or operations to execute
   * @param operationConfig    A map of config. Key is the operation name value if a Json
   *                           representation of a map.
   * @param performOperation   If this param is set to false, the function will only do a dry run
   * @return A list of MaintenanceManagementInstanceInfo
   * @throws IOException in case of network failure
   */
  public Map<String, MaintenanceManagementInstanceInfo> takeInstances(String clusterId,
      List<String> instances, List<String> healthChecks, Map<String, String> healthCheckConfig,
      List<String> operations, Map<String, String> operationConfig, boolean performOperation)
      throws IOException {
    return null;
  }

  /**
   * Perform health check and maintenance operation check and execution for a instance in
   * one cluster.
   * User need to implement OperationAbstractClass for customer operation check & execution.
   * It will invoke OperationAbstractClass.operationCheckForFreeSingleInstance and
   * OperationAbstractClass.operationExecForFreeSingleInstance.
   * The list of check and operation will be executed in the user provided sequence.
   *
   * @param clusterId          The cluster id
   * @param instanceName       The instance name
   * @param healthChecks       A list of healthChecks to perform
   * @param healthCheckConfig The input for health Checks
   * @param operations         A list of operation checks or operations to execute
   * @param operationConfig    A map of config. Key is the operation name value if a Json
   *                           representation of a map
   * @param performOperation   If this param is set to false, the function will only do a dry run
   * @return MaintenanceManagementInstanceInfo
   * @throws IOException in case of network failure
   */
  public MaintenanceManagementInstanceInfo freeInstance(String clusterId, String instanceName,
      List<String> healthChecks, Map<String, String> healthCheckConfig, List<String> operations,
      Map<String, String> operationConfig, boolean performOperation) throws IOException {

    return takeFreeSingleInstanceHelper(clusterId, instanceName, healthChecks, healthCheckConfig,
        operations, operationConfig, performOperation, false);
  }

  /**
   * Perform health check and maintenance operation check and execution for a list of instances in
   * one cluster.
   * User need to implement OperationAbstractClass for customer operation check & execution.
   * It will invoke OperationAbstractClass.operationCheckForFreeInstances and
   * OperationAbstractClass.operationExecForFreeInstances.
   * The list of check and operation will be executed in the user provided sequence.
   *
   * @param clusterId          The cluster id
   * @param instances          A list of instances
   * @param healthChecks       A list of healthChecks to perform
   * @param healthCheckConfig The input for health Checks
   * @param operations         A list of operation checks or operations to execute
   * @param operationConfig    A map of config. Key is the operation name value if a Json
   *                           representation of a map
   * @param performOperation   If this param is set to false, the function will only do a dry run
   * @return A list of MaintenanceManagementInstanceInfo
   * @throws IOException in case of network failure
   */
  public Map<String, MaintenanceManagementInstanceInfo> freeInstances(String clusterId,
      List<String> instances, List<String> healthChecks, Map<String, String> healthCheckConfig,
      List<String> operations, Map<String, String> operationConfig, boolean performOperation)
      throws IOException {
    return null;
  }

  public InstanceInfo getInstanceHealthInfo(String clusterId, String instanceName,
      List<HealthCheck> healthChecks) {
    InstanceInfo.Builder instanceInfoBuilder = new InstanceInfo.Builder(instanceName);

    InstanceConfig instanceConfig =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().instanceConfig(instanceName));
    LiveInstance liveInstance =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().liveInstance(instanceName));
    if (instanceConfig != null) {
      instanceInfoBuilder.instanceConfig(instanceConfig.getRecord());
    } else {
      LOG.warn("Missing instance config for {}", instanceName);
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
        } else {
          LOG.warn(
              "Current state is either null or partitionStateMap is missing. InstanceName: {}, SessionId: {}, ResourceName: {}",
              instanceName, sessionId, resourceName);
        }
      }
      instanceInfoBuilder.partitions(partitions);
    } else {
      LOG.warn("Missing live instance for {}", instanceName);
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

  private List<OperationInterface> getAllOperationClasses(List<String> operations) {
    List<OperationInterface> operationAbstractClassList = new ArrayList<>();
    for (String operationClassName : operations) {
      try {
        LOG.info("Loading class: " + operationClassName);
        OperationInterface userOperation =
            (OperationInterface) HelixUtil.loadClass(getClass(), operationClassName)
                .newInstance();
        operationAbstractClassList.add(userOperation);
      } catch (Exception e) {
        LOG.error("No operation class found for: {}. message: ", operationClassName, e);
        throw new HelixException(
            String.format("No operation class found for: %s. message: %s", operationClassName, e));
      }
    }
    return operationAbstractClassList;
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
  public StoppableCheck getInstanceStoppableCheck(String clusterId, String instanceName,
      String jsonContent) throws IOException {
    return batchGetInstancesStoppableChecks(clusterId, ImmutableList.of(instanceName),
        jsonContent).get(instanceName);
  }

  public Map<String, StoppableCheck> batchGetInstancesStoppableChecks(String clusterId,
      List<String> instances, String jsonContent) throws IOException {
    return batchGetInstancesStoppableChecks(clusterId, instances, jsonContent,
        Collections.emptySet());
  }

  public Map<String, StoppableCheck> batchGetInstancesStoppableChecks(String clusterId,
      List<String> instances, String jsonContent, Set<String> toBeStoppedInstances) throws IOException {
    return batchGetInstancesStoppableChecks(clusterId, instances, jsonContent, toBeStoppedInstances,
        false);
  }

  public Map<String, StoppableCheck> batchGetInstancesStoppableChecks(String clusterId,
      List<String> instances, String jsonContent, Set<String> toBeStoppedInstances,
      boolean preserveOrder) throws IOException {
    Map<String, StoppableCheck> finalStoppableChecks = new HashMap<>();
    // helix instance check.
    List<String> instancesForCustomInstanceLevelChecks =
        batchHelixInstanceStoppableCheck(clusterId, instances, finalStoppableChecks,
            toBeStoppedInstances, preserveOrder);
    // custom check, includes partition check.
    batchCustomInstanceStoppableCheck(clusterId, instancesForCustomInstanceLevelChecks,
        toBeStoppedInstances, finalStoppableChecks, getMapFromJsonPayload(jsonContent), preserveOrder);
    return finalStoppableChecks;
  }

  private MaintenanceManagementInstanceInfo takeFreeSingleInstanceHelper(String clusterId,
      String instanceName, List<String> healthChecks, Map<String, String> healthCheckConfig,
      List<String> operations, Map<String, String> operationConfig, boolean performOperation,
      boolean isTakeInstance) {

    if (operations == null) {
      operations = new ArrayList<>();
    }
    if (healthChecks == null) {
      healthChecks = new ArrayList<>();
    }

    try {
      MaintenanceManagementInstanceInfo instanceInfo;
      instanceInfo =
          batchInstanceHealthCheck(clusterId, ImmutableList.of(instanceName), healthChecks,
              healthCheckConfig).getOrDefault(instanceName, new MaintenanceManagementInstanceInfo(
              MaintenanceManagementInstanceInfo.OperationalStatus.SUCCESS));
      if (!instanceInfo.isSuccessful()) {
        return instanceInfo;
      }

      List<OperationInterface> operationAbstractClassList = getAllOperationClasses(operations);

      _dataAccessor.populateCache(OperationInterface.PROPERTY_TYPE_LIST);
      RestSnapShot sp = _dataAccessor.getRestSnapShot();
      String continueOnFailuresName =
          PerInstanceAccessor.PerInstanceProperties.continueOnFailures.name();
      Map<String, Map<String, String>> operationConfigSet = new HashMap<>();

      Map<String, String> commonOperationConfig =
          (operationConfig == null || !operationConfig.containsKey(OPERATION_CONFIG_SHARED_INPUT))
              ? Collections.emptyMap()
              : getMapFromJsonPayload(operationConfig.get(OPERATION_CONFIG_SHARED_INPUT));

      // perform operation check
      for (OperationInterface operationClass : operationAbstractClassList) {
        String operationClassName = operationClass.getClass().getName();
        Map<String, String> singleOperationConfig =
            (operationConfig == null || !operationConfig.containsKey(operationClassName))
                ? Collections.emptyMap()
                : getMapFromJsonPayload(operationConfig.get(operationClassName));
        commonOperationConfig
            .forEach(singleOperationConfig::putIfAbsent);
        operationConfigSet.put(operationClassName, singleOperationConfig);
        boolean continueOnFailures =
            singleOperationConfig.containsKey(continueOnFailuresName) && getBooleanFromJsonPayload(
                singleOperationConfig.get(continueOnFailuresName));

        MaintenanceManagementInstanceInfo checkResult = isTakeInstance ? operationClass
            .operationCheckForTakeSingleInstance(instanceName, singleOperationConfig, sp)
            : operationClass
                .operationCheckForFreeSingleInstance(instanceName, singleOperationConfig, sp);
        instanceInfo.mergeResult(checkResult, continueOnFailures);
      }

      // operation execution
      if (performOperation && instanceInfo.isSuccessful()) {
        for (OperationInterface operationClass : operationAbstractClassList) {
          Map<String, String> singleOperationConfig =
              operationConfigSet.get(operationClass.getClass().getName());
          boolean continueOnFailures =
              singleOperationConfig.containsKey(continueOnFailuresName) && Boolean
                  .parseBoolean(singleOperationConfig.get(continueOnFailuresName));
          MaintenanceManagementInstanceInfo newResult = isTakeInstance ? operationClass
              .operationExecForTakeSingleInstance(instanceName, singleOperationConfig, sp)
              : operationClass
                  .operationExecForFreeSingleInstance(instanceName, singleOperationConfig, sp);
          instanceInfo.mergeResult(newResult, continueOnFailures);
          if (!instanceInfo.isSuccessful()) {
            LOG.warn("Operation failed for {}, skip all following operations.",
                operationClass.getClass().getName());
            break;
          }
        }
      }
      return instanceInfo;
    } catch (Exception ex) {
      return new MaintenanceManagementInstanceInfo(
          MaintenanceManagementInstanceInfo.OperationalStatus.FAILURE,
          Collections.singletonList(ex.getMessage()));
    }
  }

  private List<String> batchHelixInstanceStoppableCheck(String clusterId,
      Collection<String> instances, Map<String, StoppableCheck> finalStoppableChecks,
      Set<String> toBeStoppedInstances, boolean preserveOrder) {

    // Perform all but min_active replicas check in parallel
    Map<String, Future<StoppableCheck>> helixInstanceChecks = instances.stream().collect(
        Collectors.toMap(
            Function.identity(),
            instance -> POOL.submit(() -> performHelixOwnInstanceCheck(clusterId, instance, toBeStoppedInstances)),
            (existing, replacement) -> existing,
            // Use LinkedHashMap when preserveOrder is true as we need to preserve the order of instances.
            // This is important for addMinActiveReplicaChecks which processes instances sequentially,
            // and the order of processing can affect which instances pass the min active replica check
            preserveOrder ? LinkedHashMap::new : HashMap::new
        ));

    // Perform min_active replicas check sequentially
    addMinActiveReplicaChecks(clusterId, helixInstanceChecks, toBeStoppedInstances);

    // finalStoppableChecks contains instances that does not pass this health check
    return filterInstancesForNextCheck(helixInstanceChecks, finalStoppableChecks);
  }

  private List<String> batchCustomInstanceStoppableCheck(String clusterId, List<String> instances,
      Set<String> toBeStoppedInstances, Map<String, StoppableCheck> finalStoppableChecks,
      Map<String, String> customPayLoads, boolean preserveOrder) {
    if (instances.isEmpty()) {
      // if all instances failed at previous checks, then all following checks are not required.
      return instances;
    }
    RESTConfig restConfig = _configAccessor.getRESTConfig(clusterId);
    if (restConfig == null) {
      // If the user didn't set up the rest config, we can't perform the custom check.
      // Therefore, skip the custom check.
      LOG.info(String.format("The cluster %s hasn't enabled client side health checks yet, "
          + "thus the stoppable check result is inaccurate", clusterId));
      return instances;
    }

    // Skip performing a custom check on any dead instance if the user set _skipCustomCheckIfInstanceNotAlive
    // to true.
    List<String> instanceIdsForCustomCheck = filterOutDeadInstancesIfNeeded(instances);

    // If the config has exactUrl and the CLUSTER level customer check is not skipped, we will
    // perform the custom check at cluster level.
    if (restConfig.getCompleteConfiguredHealthUrl().isPresent()) {
      if (_skipHealthCheckCategories.contains(StoppableCheck.Category.CUSTOM_AGGREGATED_CHECK)
          || instanceIdsForCustomCheck.isEmpty()) {
        return instances;
      }

      Map<String, StoppableCheck> clusterLevelCustomCheckResult =
          performAggregatedCustomCheck(clusterId, instanceIdsForCustomCheck,
              restConfig.getCompleteConfiguredHealthUrl().get(), customPayLoads,
              toBeStoppedInstances, preserveOrder);
      List<String> instancesForNextCheck = new ArrayList<>();
      clusterLevelCustomCheckResult.forEach((instance, stoppableCheck) -> {
        addStoppableCheck(finalStoppableChecks, instance, stoppableCheck);
        if (stoppableCheck.isStoppable() || isNonBlockingCheck(stoppableCheck)) {
          instancesForNextCheck.add(instance);
        }
      });

      return instancesForNextCheck;
    }

    // Reaching here means the rest config requires instances/partition level checks. We will
    // perform the custom check at instance/partition level if they are not skipped.
    List<String> instancesForCustomPartitionLevelChecks = instanceIdsForCustomCheck;
    if (!_skipHealthCheckCategories.contains(StoppableCheck.Category.CUSTOM_INSTANCE_CHECK)) {
      Map<String, Future<StoppableCheck>> customInstanceLevelChecks = instances.stream().collect(
          Collectors.toMap(
              Function.identity(),
              instance -> POOL.submit(() -> performCustomInstanceCheck(clusterId, instance, restConfig.getBaseUrl(instance), customPayLoads)),
              (existing, replacement) -> existing,
              // Use LinkedHashMap when preserveOrder is true to maintain the original order of instances
              preserveOrder ? LinkedHashMap::new : HashMap::new
          ));
      instancesForCustomPartitionLevelChecks =
          filterInstancesForNextCheck(customInstanceLevelChecks, finalStoppableChecks);
    }

    if (!instancesForCustomPartitionLevelChecks.isEmpty() && !_skipHealthCheckCategories.contains(
        StoppableCheck.Category.CUSTOM_PARTITION_CHECK)) {
      // add to finalStoppableChecks regardless of stoppable or not.
      Map<String, StoppableCheck> instancePartitionLevelChecks =
          performPartitionsCheck(instancesForCustomPartitionLevelChecks, restConfig,
              customPayLoads);
      List<String> instancesForFollowingChecks = new ArrayList<>();
      for (Map.Entry<String, StoppableCheck> instancePartitionStoppableCheckEntry : instancePartitionLevelChecks.entrySet()) {
        String instance = instancePartitionStoppableCheckEntry.getKey();
        StoppableCheck stoppableCheck = instancePartitionStoppableCheckEntry.getValue();
        addStoppableCheck(finalStoppableChecks, instance, stoppableCheck);
        if (stoppableCheck.isStoppable() || isNonBlockingCheck(stoppableCheck)) {
          // instance passed this around of check or mandatory all checks
          // will be checked in the next round
          instancesForFollowingChecks.add(instance);
        }
      }
      return instancesForFollowingChecks;
    }

    // This means that we skipped
    return instancesForCustomPartitionLevelChecks;
  }

  /**
   * Helper Methods
   * <p>
   * If users set skipCustomCheckIfInstanceNotAlive to true, filter out dead instances
   * to avoid running custom checks on them.
   *
   * @param instanceIds  the list of instances
   * @return either the original list or a filtered list of only live instances
   */
  private List<String> filterOutDeadInstancesIfNeeded(List<String> instanceIds) {
    if (!_skipCustomChecksIfNoLiveness) {
      // We are not skipping the not-alive check, so just return all instances.
      return instanceIds;
    }

    // Retrieve the set of currently live instances
    PropertyKey.Builder keyBuilder = _dataAccessor.keyBuilder();
    List<String> liveNodes = _dataAccessor.getChildNames(keyBuilder.liveInstances());

    // Filter out instances that are not in the live list
    List<String> filtered = new ArrayList<>();
    List<String> skipped = new ArrayList<>();
    for (String instanceId : instanceIds) {
      if (liveNodes.contains(instanceId)) {
        filtered.add(instanceId);
      } else {
        skipped.add(instanceId);
      }
    }

    if (!skipped.isEmpty()) {
      LOG.info("Skipping any custom checks for instances due to liveness: {}", skipped);
    }
    return filtered;
  }

  private Map<String, MaintenanceManagementInstanceInfo> batchInstanceHealthCheck(String clusterId,
      List<String> instances, List<String> healthChecks, Map<String, String> healthCheckConfig) {
    List<String> instancesForNext = new ArrayList<>(instances);
    Map<String, MaintenanceManagementInstanceInfo> instanceInfos = new HashMap<>();
    Map<String, StoppableCheck> finalStoppableChecks = new HashMap<>();
    // TODO: Right now user can only choose from HelixInstanceStoppableCheck and
    // CustomInstanceStoppableCheck. We should add finer grain check groups to choose from
    // i.e. HELIX:INSTANCE_NOT_ENABLED, CUSTOM_PARTITION_HEALTH_FAILURE:PARTITION_INITIAL_STATE_FAIL etc.
    for (String healthCheck : healthChecks) {
      if (healthCheck.equals(HELIX_INSTANCE_STOPPABLE_CHECK)) {
        // this is helix own check
        instancesForNext =
            batchHelixInstanceStoppableCheck(clusterId, instancesForNext, finalStoppableChecks,
                Collections.emptySet(), false);
      } else if (healthCheck.equals(HELIX_CUSTOM_STOPPABLE_CHECK)) {
        // custom check, includes custom Instance check and partition check.
        instancesForNext =
            batchCustomInstanceStoppableCheck(clusterId, instancesForNext, Collections.emptySet(),
                finalStoppableChecks, healthCheckConfig, false);
      } else {
        throw new UnsupportedOperationException(healthCheck + " is not supported yet!");
      }
    }
    // assemble result. Returned map contains all instances with pass or fail status.
    Set<String> clearedInstance = new HashSet<>(instancesForNext);
    for (String instance : instances) {
      MaintenanceManagementInstanceInfo result = new MaintenanceManagementInstanceInfo(
          clearedInstance.contains(instance)
              ? MaintenanceManagementInstanceInfo.OperationalStatus.SUCCESS
              : MaintenanceManagementInstanceInfo.OperationalStatus.FAILURE);
      if (finalStoppableChecks.containsKey(instance) && !finalStoppableChecks.get(instance)
          .isStoppable()) {
        // If an non blocking check failed, the we will have a stoppbale check object with
        // stoppbale = false and the instance is in clearedInstance. We will sign Success state and
        // a error message.
        result.addMessages(finalStoppableChecks.get(instance).getFailedChecks());
      }
      instanceInfos.put(instance, result);
    }
    return instanceInfos;
  }

  private void addStoppableCheck(Map<String, StoppableCheck> stoppableChecks, String instance,
      StoppableCheck stoppableCheck) {
    if (!stoppableChecks.containsKey(instance)) {
      stoppableChecks.put(instance, stoppableCheck);
    } else {
      // Merge two checks
      stoppableChecks.get(instance).add(stoppableCheck);
    }
  }

  private List<String> filterInstancesForNextCheck(
      Map<String, Future<StoppableCheck>> futureStoppableCheckByInstance,
      Map<String, StoppableCheck> finalStoppableCheckByInstance) {
    List<String> instancesForNextCheck = new ArrayList<>();
    for (Map.Entry<String, Future<StoppableCheck>> entry : futureStoppableCheckByInstance
        .entrySet()) {
      String instance = entry.getKey();
      try {
        StoppableCheck stoppableCheck = entry.getValue().get();
        addStoppableCheck(finalStoppableCheckByInstance, instance, stoppableCheck);
        if (stoppableCheck.isStoppable() || isNonBlockingCheck(stoppableCheck)) {
          // instance passed this around of check or mandatory all checks
          // will be checked in the next round
          instancesForNextCheck.add(instance);
        }
      } catch (Exception e) {
        String errorMessage =
            String.format("Failed to get StoppableChecks in parallel. Instance: %s", instance);
        LOG.error(errorMessage, e);
        throw new HelixException(errorMessage);
      }
    }

    return instancesForNextCheck;
  }

  private boolean isNonBlockingCheck(StoppableCheck stoppableCheck) {
    if (_nonBlockingHealthChecks.isEmpty()) {
      return false;
    }
    if (_nonBlockingHealthChecks.contains(ALL_HEALTH_CHECK_NONBLOCK)) {
      return true;
    }
    for (String failedCheck : stoppableCheck.getFailedChecks()) {
      if (failedCheck.startsWith("CUSTOM_")) {
        // failed custom check will have the pattern
        // "CUSTOM_PARTITION_HEALTH_FAILURE:PARTITION_INITIAL_STATE_FAIL:partition_name"
        // we want to keep the first 2 parts as failed test name.
        String[] checks = failedCheck.split(":", 3);
        failedCheck = checks[0] + ":" + checks[1];
      }
      // Helix own health check name will be in this pattern "HELIX:INSTANCE_NOT_ALIVE",
      // no need to preprocess.
      if (!_nonBlockingHealthChecks.contains(failedCheck)) {
        return false;
      }
    }
    return true;
  }

  private StoppableCheck performHelixOwnInstanceCheck(String clusterId, String instanceName,
      Set<String> toBeStoppedInstances) {
    LOG.info("Perform helix own custom health checks for {}/{}", clusterId, instanceName);
    List<HealthCheck> healthChecksToExecute = new ArrayList<>(HealthCheck.STOPPABLE_CHECK_LIST);
    healthChecksToExecute.removeAll(_skipStoppableHealthCheckList);
    // Min active check is performed sequentially later
    healthChecksToExecute.remove(HealthCheck.MIN_ACTIVE_REPLICA_CHECK_FAILED);
    Map<String, Boolean> helixStoppableCheck =
        getInstanceHealthStatus(clusterId, instanceName, healthChecksToExecute,
            toBeStoppedInstances);

    return new StoppableCheck(helixStoppableCheck, StoppableCheck.Category.HELIX_OWN_CHECK);
  }

  private StoppableCheck performCustomInstanceCheck(String clusterId, String instanceName,
      String baseUrl, Map<String, String> customPayLoads) {
    LOG.info("Perform instance level client side health checks for {}/{}", clusterId, instanceName);
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(_namespace);

    // Total requests metric is included as an attribute(Count) in timers
    try (final Timer.Context timer = metrics.timer(CUSTOM_INSTANCE_CHECK_HTTP_REQUESTS_DURATION)
        .time()) {
      Map<String, Boolean> instanceStoppableCheck =
          _customRestClient.getInstanceStoppableCheck(baseUrl, customPayLoads);
      return new StoppableCheck(instanceStoppableCheck,
          StoppableCheck.Category.CUSTOM_INSTANCE_CHECK);
    } catch (IOException ex) {
      LOG.error("Custom client side instance level health check for {}/{} failed.", clusterId,
          instanceName, ex);
      metrics.counter(CUSTOM_INSTANCE_CHECK_HTTP_REQUESTS_ERROR_TOTAL).inc();
      return new StoppableCheck(false, Collections.singletonList(instanceName),
          StoppableCheck.Category.CUSTOM_INSTANCE_CHECK);
    }
  }

  private Map<String, StoppableCheck> performPartitionsCheck(List<String> instances,
      RESTConfig restConfig, Map<String, String> customPayLoads) {
    Map<String, Map<String, Boolean>> allPartitionsHealthOnLiveInstance =
        _dataAccessor.getAllPartitionsHealthOnLiveInstance(restConfig, customPayLoads, _skipZKRead);
    List<ExternalView> externalViews =
        _dataAccessor.getChildValues(_dataAccessor.keyBuilder().externalViews(), true);
    Map<String, StoppableCheck> instanceStoppableChecks = new HashMap<>();
    for (String instanceName : instances) {
      Map<String, List<String>> unHealthyPartitions = InstanceValidationUtil
          .perPartitionHealthCheck(externalViews, allPartitionsHealthOnLiveInstance, instanceName,
              _dataAccessor);

      List<String> unHealthyPartitionsList = new ArrayList<>();
      for (String partitionName : unHealthyPartitions.keySet()) {
        for (String reason : unHealthyPartitions.get(partitionName)) {
          unHealthyPartitionsList.add(reason.toUpperCase() + ":" + partitionName);
        }
      }
      StoppableCheck stoppableCheck = new StoppableCheck(unHealthyPartitionsList.isEmpty(),
          unHealthyPartitionsList, StoppableCheck.Category.CUSTOM_PARTITION_CHECK);
      instanceStoppableChecks.put(instanceName, stoppableCheck);
    }

    return instanceStoppableChecks;
  }

  private Map<String, StoppableCheck> performAggregatedCustomCheck(String clusterId,
      List<String> instances, String url, Map<String, String> customPayLoads,
      Set<String> toBeStoppedInstances, boolean preserveOrder) {
    // Use LinkedHashMap when preserveOrder is true to maintain the original order of instances
    Map<String, StoppableCheck> aggregatedStoppableChecks = preserveOrder ?
        new LinkedHashMap<>() : new HashMap<>();
    try {
      Map<String, List<String>> customCheckResult =
          _customRestClient.getAggregatedStoppableCheck(url, instances, toBeStoppedInstances,
              clusterId, customPayLoads);
      for (Map.Entry<String, List<String>> entry : customCheckResult.entrySet()) {
        // If the list is empty, it means the instance is stoppable.
        aggregatedStoppableChecks.put(entry.getKey(),
            new StoppableCheck(entry.getValue().isEmpty(), entry.getValue(),
                StoppableCheck.Category.CUSTOM_AGGREGATED_CHECK));
      }
    } catch (IOException ex) {
      LOG.error("Custom client side aggregated health check for {} failed.", clusterId, ex);
      return instances.stream().collect(Collectors.toMap(
          Function.identity(),
          instance -> new StoppableCheck(false, Collections.singletonList(instance),
              StoppableCheck.Category.CUSTOM_AGGREGATED_CHECK),
          (existing, replacement) -> existing,
          // Use LinkedHashMap when preserveOrder is true to maintain the original order of instances
          preserveOrder ? LinkedHashMap::new : HashMap::new));
    }
    return aggregatedStoppableChecks;
  }

  public static Map<String, String> getMapFromJsonPayload(String jsonContent) throws IOException {
    Map<String, String> result = new HashMap<>();
    if (jsonContent == null) {
      return result;
    }
    JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonContent);
    // parsing the inputs as string key value pairs
    jsonNode.fields().forEachRemaining(kv -> result.put(kv.getKey(),
        kv.getValue().isValueNode() ? kv.getValue().asText() : kv.getValue().toString()));
    return result;
  }

  public static Map<String, String> getMapFromJsonPayload(JsonNode jsonNode)
      throws IllegalArgumentException {
    Map<String, String> result = new HashMap<>();
    if (jsonNode != null) {
      jsonNode.fields().forEachRemaining(kv -> result.put(kv.getKey(),
          kv.getValue().isValueNode() ? kv.getValue().asText() : kv.getValue().toString()));
    }
    return result;
  }

  public static List<String> getListFromJsonPayload(JsonNode jsonContent)
      throws IllegalArgumentException {
    return (jsonContent == null) ? Collections.emptyList()
        : OBJECT_MAPPER.convertValue(jsonContent, List.class);
  }

  public static List<String> getListFromJsonPayload(String jsonString)
      throws IllegalArgumentException, JsonProcessingException {
    return (jsonString == null) ? Collections.emptyList()
        : OBJECT_MAPPER.readValue(jsonString, List.class);
  }

  public static boolean getBooleanFromJsonPayload(String jsonString)
      throws IllegalArgumentException, JsonProcessingException {
    return  OBJECT_MAPPER.readTree(jsonString).asBoolean();
  }

  @VisibleForTesting
  protected Map<String, Boolean> getInstanceHealthStatus(String clusterId, String instanceName,
      List<HealthCheck> healthChecks) {
    return getInstanceHealthStatus(clusterId, instanceName, healthChecks, Collections.emptySet());
  }

  @VisibleForTesting
  protected Map<String, Boolean> getInstanceHealthStatus(String clusterId, String instanceName,
      List<HealthCheck> healthChecks, Set<String> toBeStoppedInstances) {
    Map<String, Boolean> healthStatus = new HashMap<>();
    for (HealthCheck healthCheck : healthChecks) {
      switch (healthCheck) {
        case INVALID_CONFIG:
          boolean validConfig;
          try {
            validConfig =
                InstanceValidationUtil.hasValidConfig(_dataAccessor, clusterId, instanceName);
          } catch (HelixException e) {
            validConfig = false;
            LOG.warn("Cluster {} instance {} doesn't have valid config: {}", clusterId, instanceName,
                e.getMessage());
          }

          // TODO: should add reason to request response
          healthStatus.put(HealthCheck.INVALID_CONFIG.name(), validConfig);
          if (!validConfig) {
            // No need to do remaining health checks.
            return healthStatus;
          }
          break;
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
              InstanceValidationUtil.isResourceAssigned(_dataAccessor, instanceName));
          break;
        case MIN_ACTIVE_REPLICA_CHECK_FAILED:
          healthStatus.put(HealthCheck.MIN_ACTIVE_REPLICA_CHECK_FAILED.name(),
              InstanceValidationUtil.siblingNodesActiveReplicaCheck(_dataAccessor, instanceName, toBeStoppedInstances));
          break;
        default:
          LOG.error("Unsupported health check: {}", healthCheck);
          break;
      }
    }

    return healthStatus;
  }

  // Adds the result of the min_active replica check for each stoppable check passed in futureStoppableCheckByInstance
  private void addMinActiveReplicaChecks(String clusterId, Map<String, Future<StoppableCheck>> futureStoppableCheckByInstance,
      Set<String> toBeStoppedInstances) {
    // Do not perform check if in the skip list
    if (_skipStoppableHealthCheckList.contains(HealthCheck.MIN_ACTIVE_REPLICA_CHECK_FAILED)) {
      return;
    }

    Set<String> possibleToStopInstances = new HashSet<>(toBeStoppedInstances);
    for (Map.Entry<String, Future<StoppableCheck>> entry : futureStoppableCheckByInstance.entrySet()) {
      try {
        String instanceName = entry.getKey();
        StoppableCheck stoppableCheck = entry.getValue().get();

        // Check if min active will be violated and add to stoppableCheck. If instance still stoppable,
        // add to possibleToStopInstances
        Map<String, Boolean> helixStoppableCheck =
            getInstanceHealthStatus(clusterId, instanceName,
                Collections.singletonList(HealthCheck.MIN_ACTIVE_REPLICA_CHECK_FAILED),
                possibleToStopInstances);
        stoppableCheck.add(new StoppableCheck(helixStoppableCheck, StoppableCheck.Category.HELIX_OWN_CHECK));

        if (stoppableCheck.isStoppable()) {
          possibleToStopInstances.add(instanceName);
        }

      } catch (Exception e) {
        String errorMessage = String.format("Failed to get StoppableChecks in parallel. Instances: %s",
            futureStoppableCheckByInstance.values());
        LOG.error(errorMessage, e);
        throw new HelixException(errorMessage);
      }
    }
  }

  public static class MaintenanceManagementServiceBuilder {
    private ConfigAccessor _configAccessor;
    private boolean _skipZKRead;
    private boolean _skipCustomChecksIfNoLiveness = false;
    private String _namespace;
    private ZKHelixDataAccessor _dataAccessor;
    private CustomRestClient _customRestClient;
    private Set<String> _nonBlockingHealthChecks;
    private Set<StoppableCheck.Category> _skipHealthCheckCategories = Collections.emptySet();
    private List<HealthCheck> _skipStoppableHealthCheckList = Collections.emptyList();

    public MaintenanceManagementServiceBuilder setConfigAccessor(ConfigAccessor configAccessor) {
      _configAccessor = configAccessor;
      return this;
    }

    public MaintenanceManagementServiceBuilder setSkipZKRead(boolean skipZKRead) {
      _skipZKRead = skipZKRead;
      return this;
    }

    public MaintenanceManagementServiceBuilder setNamespace(String namespace) {
      _namespace = namespace;
      return this;
    }

    public MaintenanceManagementServiceBuilder setDataAccessor(
        ZKHelixDataAccessor dataAccessor) {
      _dataAccessor = dataAccessor;
      return this;
    }

    public MaintenanceManagementServiceBuilder setCustomRestClient(
        CustomRestClient customRestClient) {
      _customRestClient = customRestClient;
      return this;
    }

    public MaintenanceManagementServiceBuilder setNonBlockingHealthChecks(
        Set<String> nonBlockingHealthChecks) {
      _nonBlockingHealthChecks = nonBlockingHealthChecks;
      return this;
    }

    public MaintenanceManagementServiceBuilder setSkipHealthCheckCategories(
        Set<StoppableCheck.Category> skipHealthCheckCategories) {
      _skipHealthCheckCategories = skipHealthCheckCategories;
      return this;
    }

    public MaintenanceManagementServiceBuilder setSkipStoppableHealthCheckList(
        List<HealthCheck> skipStoppableHealthCheckList) {
      _skipStoppableHealthCheckList = skipStoppableHealthCheckList;
      return this;
    }

    public MaintenanceManagementServiceBuilder setSkipCustomChecksIfNoLiveness(
        boolean skipCustomChecksIfNoLiveness) {
      _skipCustomChecksIfNoLiveness = skipCustomChecksIfNoLiveness;
      return this;
    }

    public MaintenanceManagementService build() {
      validate();
      return new MaintenanceManagementService(_dataAccessor, _configAccessor, _customRestClient,
          _skipZKRead, _nonBlockingHealthChecks, _skipHealthCheckCategories,
          _skipStoppableHealthCheckList, _namespace, _skipCustomChecksIfNoLiveness);
    }

    private void validate() throws IllegalArgumentException {
      List<String> msg = new ArrayList<>();
      if (_configAccessor == null) {
        msg.add("'configAccessor' can't be null.");
      }
      if (_namespace == null) {
        msg.add("'namespace' can't be null.");
      }
      if (_dataAccessor == null) {
        msg.add("'_dataAccessor' can't be null.");
      }
      if (_customRestClient == null) {
        msg.add("'customRestClient' can't be null.");
      }
      if (msg.size() != 0) {
        throw new IllegalArgumentException(
            "One or more mandatory arguments are not set " + msg);
      }
    }
  }
}

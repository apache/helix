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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
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

  private final ConfigAccessor _configAccessor;
  private final CustomRestClient _customRestClient;
  private final String _namespace;
  private final boolean _skipZKRead;
  private final HelixDataAccessorWrapper _dataAccessor;
  private final Set<String> _nonBlockingHealthChecks;

  public MaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
      ConfigAccessor configAccessor, boolean skipZKRead, String namespace) {
    this(dataAccessor, configAccessor, CustomRestClientFactory.get(), skipZKRead,
        Collections.emptySet(), namespace);
  }

  public MaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
      ConfigAccessor configAccessor, boolean skipZKRead, Set<String> nonBlockingHealthChecks,
      String namespace) {
    this(dataAccessor, configAccessor, CustomRestClientFactory.get(), skipZKRead,
        nonBlockingHealthChecks, namespace);
  }

  public MaintenanceManagementService(ZKHelixDataAccessor dataAccessor,
      ConfigAccessor configAccessor, boolean skipZKRead, boolean continueOnFailure,
      String namespace) {
    this(dataAccessor, configAccessor, CustomRestClientFactory.get(), skipZKRead,
        continueOnFailure ? Collections.singleton(ALL_HEALTH_CHECK_NONBLOCK)
            : Collections.emptySet(), namespace);
  }

  @VisibleForTesting
  MaintenanceManagementService(ZKHelixDataAccessor dataAccessor, ConfigAccessor configAccessor,
      CustomRestClient customRestClient, boolean skipZKRead, Set<String> nonBlockingHealthChecks,
      String namespace) {
    _dataAccessor = new HelixDataAccessorWrapper(dataAccessor, customRestClient, namespace);
    _configAccessor = configAccessor;
    _customRestClient = customRestClient;
    _skipZKRead = skipZKRead;
    _nonBlockingHealthChecks = nonBlockingHealthChecks;
    _namespace = namespace;
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
    return batchGetInstancesStoppableChecks(clusterId, ImmutableList.of(instanceName), jsonContent)
        .get(instanceName);
  }


  public Map<String, StoppableCheck> batchGetInstancesStoppableChecks(String clusterId,
      List<String> instances, String jsonContent) throws IOException {
    Map<String, StoppableCheck> finalStoppableChecks = new HashMap<>();
    // helix instance check.
    List<String> instancesForCustomInstanceLevelChecks =
        batchHelixInstanceStoppableCheck(clusterId, instances, finalStoppableChecks);
    // custom check, includes partition check.
    batchCustomInstanceStoppableCheck(clusterId, instancesForCustomInstanceLevelChecks,
        finalStoppableChecks, getMapFromJsonPayload(jsonContent));
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
      Collection<String> instances, Map<String, StoppableCheck> finalStoppableChecks) {
    Map<String, Future<StoppableCheck>> helixInstanceChecks = instances.stream().collect(Collectors
        .toMap(Function.identity(),
            instance -> POOL.submit(() -> performHelixOwnInstanceCheck(clusterId, instance))));
    // finalStoppableChecks contains instances that does not pass this health check
    return filterInstancesForNextCheck(helixInstanceChecks, finalStoppableChecks);
  }

  private List<String> batchCustomInstanceStoppableCheck(String clusterId, List<String> instances,
      Map<String, StoppableCheck> finalStoppableChecks, Map<String, String> customPayLoads) {
    if (instances.isEmpty()) {
      // if all instances failed at previous checks, then all following checks are not required.
      return instances;
    }
    RESTConfig restConfig = _configAccessor.getRESTConfig(clusterId);
    if (restConfig == null) {
      String errorMessage = String.format(
          "The cluster %s hasn't enabled client side health checks yet, "
              + "thus the stoppable check result is inaccurate", clusterId);
      LOG.error(errorMessage);
      throw new HelixException(errorMessage);
    }
    Map<String, Future<StoppableCheck>> customInstanceLevelChecks = instances.stream().collect(
        Collectors.toMap(Function.identity(), instance -> POOL.submit(
            () -> performCustomInstanceCheck(clusterId, instance, restConfig.getBaseUrl(instance),
                customPayLoads))));
    List<String> instancesForCustomPartitionLevelChecks =
        filterInstancesForNextCheck(customInstanceLevelChecks, finalStoppableChecks);
    if (!instancesForCustomPartitionLevelChecks.isEmpty()) {
      // add to finalStoppableChecks regardless of stoppable or not.
      Map<String, StoppableCheck> instancePartitionLevelChecks =
          performPartitionsCheck(instancesForCustomPartitionLevelChecks, restConfig,
              customPayLoads);
      List<String> instancesForFollowingChecks = new ArrayList<>();
      for (Map.Entry<String, StoppableCheck> instancePartitionStoppableCheckEntry : instancePartitionLevelChecks
          .entrySet()) {
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
    return instancesForCustomPartitionLevelChecks;
  }

  private Map<String, MaintenanceManagementInstanceInfo> batchInstanceHealthCheck(String clusterId,
      List<String> instances, List<String> healthChecks, Map<String, String> healthCheckConfig) {
    List<String> instancesForNext = new ArrayList<>(instances);
    Map<String, MaintenanceManagementInstanceInfo> instanceInfos = new HashMap<>();
    Map<String, StoppableCheck> finalStoppableChecks = new HashMap<>();
    // TODO: Right now user can only choose from HelixInstanceStoppableCheck and
    // CostumeInstanceStoppableCheck. We should add finer grain check groups to choose from
    // i.e. HELIX:INSTANCE_NOT_ENABLED, CUSTOM_PARTITION_HEALTH_FAILURE:PARTITION_INITIAL_STATE_FAIL etc.
    for (String healthCheck : healthChecks) {
      if (healthCheck.equals(HELIX_INSTANCE_STOPPABLE_CHECK)) {
        // this is helix own check
        instancesForNext =
            batchHelixInstanceStoppableCheck(clusterId, instancesForNext, finalStoppableChecks);
      } else if (healthCheck.equals(HELIX_CUSTOM_STOPPABLE_CHECK)) {
        // custom check, includes custom Instance check and partition check.
        instancesForNext =
            batchCustomInstanceStoppableCheck(clusterId, instancesForNext, finalStoppableChecks,
                healthCheckConfig);
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
        if (!stoppableCheck.isStoppable()) {
          // put the check result of the failed-to-stop instances
          addStoppableCheck(finalStoppableCheckByInstance, instance, stoppableCheck);
        }
        if (stoppableCheck.isStoppable() || isNonBlockingCheck(stoppableCheck)) {
          // instance passed this around of check or mandatory all checks
          // will be checked in the next round
          instancesForNextCheck.add(instance);
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Failed to get StoppableChecks in parallel. Instance: {}", instance, e);
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

  private StoppableCheck performHelixOwnInstanceCheck(String clusterId, String instanceName) {
    LOG.info("Perform helix own custom health checks for {}/{}", clusterId, instanceName);
    Map<String, Boolean> helixStoppableCheck =
        getInstanceHealthStatus(clusterId, instanceName, HealthCheck.STOPPABLE_CHECK_LIST);

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
              InstanceValidationUtil.siblingNodesActiveReplicaCheck(_dataAccessor, instanceName));
          break;
        default:
          LOG.error("Unsupported health check: {}", healthCheck);
          break;
      }
    }

    return healthStatus;
  }
}

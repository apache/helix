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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

public class InstanceUtil {

  // Private constructor to prevent instantiation
  private InstanceUtil() {
  }

  // Validators for instance operation transitions
  private static final InstanceOperationValidator ALWAYS_ALLOWED =
      (baseDataAccessor, configAccessor, clusterName, instanceConfig) -> true;
  private static final InstanceOperationValidator ALL_MATCHES_ARE_UNKNOWN =
      (baseDataAccessor, configAccessor, clusterName, instanceConfig) -> {
        List<InstanceConfig> matchingInstances =
            findInstancesWithMatchingLogicalId(baseDataAccessor, configAccessor, clusterName,
                instanceConfig);
        return matchingInstances.isEmpty() || matchingInstances.stream().allMatch(
            instance -> instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.UNKNOWN));
      };
  private static final InstanceOperationValidator ALL_MATCHES_ARE_UNKNOWN_OR_EVACUATE =
      (baseDataAccessor, configAccessor, clusterName, instanceConfig) -> {
        List<InstanceConfig> matchingInstances =
            findInstancesWithMatchingLogicalId(baseDataAccessor, configAccessor, clusterName,
                instanceConfig);
        return matchingInstances.isEmpty() || matchingInstances.stream().allMatch(instance ->
            instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.UNKNOWN)
                || instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.EVACUATE));
      };
  private static final InstanceOperationValidator ANY_MATCH_ENABLE_OR_DISABLE =
      (baseDataAccessor, configAccessor, clusterName, instanceConfig) -> {
        List<InstanceConfig> matchingInstances =
            findInstancesWithMatchingLogicalId(baseDataAccessor, configAccessor, clusterName,
                instanceConfig);
        return !matchingInstances.isEmpty() && matchingInstances.stream().anyMatch(instance ->
            instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.ENABLE)
                || instance.getInstanceOperation().getOperation()
                .equals(InstanceConstants.InstanceOperation.DISABLE));
      };

  // Validator map for valid instance operation transitions <currentOperation>:<targetOperation>:<validator>
  private static final ImmutableMap<InstanceConstants.InstanceOperation, ImmutableMap<InstanceConstants.InstanceOperation, InstanceOperationValidator>>
      VALID_INSTANCE_OPERATION_TRANSITIONS =
      ImmutableMap.of(InstanceConstants.InstanceOperation.ENABLE,
      // ENABLE and DISABLE can be set to UNKNOWN when matching instance is in SWAP_IN and set to ENABLE in a transaction.
          ImmutableMap.of(InstanceConstants.InstanceOperation.ENABLE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.DISABLE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.EVACUATE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED),
      InstanceConstants.InstanceOperation.DISABLE,
          ImmutableMap.of(InstanceConstants.InstanceOperation.DISABLE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.ENABLE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.EVACUATE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED),
      InstanceConstants.InstanceOperation.SWAP_IN,
      // SWAP_IN can be set to ENABLE when matching instance is in UNKNOWN state in a transaction.
          ImmutableMap.of(InstanceConstants.InstanceOperation.SWAP_IN, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED),
      InstanceConstants.InstanceOperation.EVACUATE,
          ImmutableMap.of(InstanceConstants.InstanceOperation.EVACUATE, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.ENABLE, ALL_MATCHES_ARE_UNKNOWN,
          InstanceConstants.InstanceOperation.DISABLE, ALL_MATCHES_ARE_UNKNOWN,
          InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED),
      InstanceConstants.InstanceOperation.UNKNOWN,
          ImmutableMap.of(InstanceConstants.InstanceOperation.UNKNOWN, ALWAYS_ALLOWED,
          InstanceConstants.InstanceOperation.ENABLE, ALL_MATCHES_ARE_UNKNOWN_OR_EVACUATE,
          InstanceConstants.InstanceOperation.DISABLE, ALL_MATCHES_ARE_UNKNOWN_OR_EVACUATE,
          InstanceConstants.InstanceOperation.SWAP_IN, ANY_MATCH_ENABLE_OR_DISABLE));

  /**
   * Validates if the transition from the current operation to the target operation is valid.
   *
   * @param configAccessor   The ConfigAccessor instance
   * @param clusterName      The cluster name
   * @param instanceConfig   The current instance configuration
   * @param currentOperation The current operation
   * @param targetOperation  The target operation
   * @deprecated Use {@link #validateInstanceOperationTransition(BaseDataAccessor, String, InstanceConfig, InstanceConstants.InstanceOperation, InstanceConstants.InstanceOperation)}
   *        instead for better performance.
   */
  @Deprecated
  public static void validateInstanceOperationTransition(ConfigAccessor configAccessor,
      String clusterName, InstanceConfig instanceConfig,
      InstanceConstants.InstanceOperation currentOperation,
      InstanceConstants.InstanceOperation targetOperation) {

    validateInstanceOperationTransition(null, configAccessor, clusterName, instanceConfig,
        currentOperation, targetOperation);
  }

  /**
   * Validates if the transition from the current operation to the target operation is valid.
   *
   * @param baseDataAccessor The BaseDataAccessor instance
   * @param clusterName      The cluster name
   * @param instanceConfig   The current instance configuration
   * @param currentOperation The current operation
   * @param targetOperation  The target operation
   */
  public static void validateInstanceOperationTransition(
      BaseDataAccessor<ZNRecord> baseDataAccessor, String clusterName,
      InstanceConfig instanceConfig,
      InstanceConstants.InstanceOperation currentOperation,
      InstanceConstants.InstanceOperation targetOperation) {

    validateInstanceOperationTransition(baseDataAccessor, null, clusterName, instanceConfig,
        currentOperation, targetOperation);
  }

  private static void validateInstanceOperationTransition(
      @Nullable BaseDataAccessor<ZNRecord> baseDataAccessor,
      @Nullable ConfigAccessor configAccessor, String clusterName, InstanceConfig instanceConfig,
      InstanceConstants.InstanceOperation currentOperation,
      InstanceConstants.InstanceOperation targetOperation) {
    ImmutableMap<InstanceConstants.InstanceOperation, InstanceOperationValidator> transitionMap =
        VALID_INSTANCE_OPERATION_TRANSITIONS.get(currentOperation);

    if (transitionMap == null || !transitionMap.containsKey(targetOperation)) {
      throw new HelixException(
          "Invalid instance operation transition from " + currentOperation + " to "
              + targetOperation);
    }

    InstanceOperationValidator validator = transitionMap.get(targetOperation);
    if (validator == null || !validator.validate(baseDataAccessor, configAccessor, clusterName,
        instanceConfig)) {
      throw new HelixException(
          "Failed validation for instance operation transition from " + currentOperation + " to "
              + targetOperation);
    }
  }

  /**
   * Finds the instances that have a matching logical ID with the given instance.
   *
   * @param configAccessor  The ConfigAccessor instance
   * @param clusterName     The cluster name
   * @param instanceConfig  The instance configuration to match
   * @return A list of matching instances
   */
  @Deprecated
  public static List<InstanceConfig> findInstancesWithMatchingLogicalId(
      ConfigAccessor configAccessor, String clusterName, InstanceConfig instanceConfig) {
    String logicalIdKey =
        ClusterTopologyConfig.createFromClusterConfig(configAccessor.getClusterConfig(clusterName))
            .getEndNodeType();

    // Retrieve and filter instances with matching logical ID
    return configAccessor.getKeys(
            new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT,
                clusterName).build()).stream()
        .map(instanceName -> configAccessor.getInstanceConfig(clusterName, instanceName)).filter(
            potentialInstanceConfig ->
                !potentialInstanceConfig.getInstanceName().equals(instanceConfig.getInstanceName())
                    && potentialInstanceConfig.getLogicalId(logicalIdKey)
                    .equals(instanceConfig.getLogicalId(logicalIdKey)))
        .collect(Collectors.toList());
  }

  /**
   * Finds the instances that have a matching logical ID with the given instance.
   *
   * @param clusterName    The cluster name
   * @param instanceConfig The instance configuration to match
   * @return A list of matching instances
   */
  public static List<InstanceConfig> findInstancesWithMatchingLogicalId(
      BaseDataAccessor<ZNRecord> baseDataAccessor, String clusterName,
      InstanceConfig instanceConfig) {
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(clusterName, baseDataAccessor);

    ClusterConfig clusterConfig =
        helixDataAccessor.getProperty(helixDataAccessor.keyBuilder().clusterConfig());
    String logicalIdKey =
        ClusterTopologyConfig.createFromClusterConfig(clusterConfig).getEndNodeType();

    List<InstanceConfig> instanceConfigs =
        helixDataAccessor.getChildValues(helixDataAccessor.keyBuilder().instanceConfigs(), true);

    // Retrieve and filter instances with matching logical ID
    return instanceConfigs.stream().filter(potentialInstanceConfig ->
        !potentialInstanceConfig.getInstanceName().equals(instanceConfig.getInstanceName())
            && potentialInstanceConfig.getLogicalId(logicalIdKey)
            .equals(instanceConfig.getLogicalId(logicalIdKey))).collect(Collectors.toList());
  }

  private static List<InstanceConfig> findInstancesWithMatchingLogicalId(
      @Nullable BaseDataAccessor<ZNRecord> baseDataAccessor,
      @Nullable ConfigAccessor configAccessor, String clusterName, InstanceConfig instanceConfig) {
    if (baseDataAccessor == null && configAccessor == null) {
      throw new HelixException(
          "Both BaseDataAccessor and ConfigAccessor cannot be null at the same time");
    }

    return baseDataAccessor != null ? findInstancesWithMatchingLogicalId(baseDataAccessor,
        clusterName, instanceConfig)
        : findInstancesWithMatchingLogicalId(configAccessor, clusterName, instanceConfig);
  }

  /**
   * Sets the instance operation for the given instance.
   *
   * @param configAccessor      The ConfigAccessor instance
   * @param baseDataAccessor    The BaseDataAccessor instance
   * @param clusterName         The cluster name
   * @param instanceName        The instance name
   * @param instanceOperation   The instance operation to set
   */
  public static void setInstanceOperation(ConfigAccessor configAccessor,
      BaseDataAccessor<ZNRecord> baseDataAccessor, String clusterName, String instanceName,
      InstanceConfig.InstanceOperation instanceOperation) {
    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);

    // Retrieve the current instance configuration
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new HelixException("Cluster " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    // Validate the instance operation transition
    validateInstanceOperationTransition(baseDataAccessor, configAccessor, clusterName,
        instanceConfig,
        instanceConfig.getInstanceOperation().getOperation(),
        instanceOperation == null ? InstanceConstants.InstanceOperation.ENABLE
            : instanceOperation.getOperation());

    // Update the instance operation
    boolean succeeded = baseDataAccessor.update(path, currentData -> {
      if (currentData == null) {
        throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
            + ", participant config is null");
      }

      InstanceConfig config = new InstanceConfig(currentData);
      config.setInstanceOperation(instanceOperation);
      return config.getRecord();
    }, AccessOption.PERSISTENT);

    if (!succeeded) {
      throw new HelixException(
          "Failed to update instance operation. Please check if instance is disabled.");
    }
  }

  private interface InstanceOperationValidator {
    boolean validate(@Nullable BaseDataAccessor<ZNRecord> baseDataAccessor,
        @Nullable ConfigAccessor configAccessor, String clusterName, InstanceConfig instanceConfig);
  }
}

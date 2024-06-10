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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;

public class InstanceUtil {

  // Private constructor to prevent instantiation
  private InstanceUtil() {
  }

  // Validators for instance operation transitions
  private static final Function<List<InstanceConfig>, Boolean> ALWAYS_ALLOWED =
      (matchingInstances) -> true;
  private static final Function<List<InstanceConfig>, Boolean> ALL_MATCHES_ARE_UNKNOWN =
      (matchingInstances) -> matchingInstances.isEmpty() || matchingInstances.stream().allMatch(
          instance -> instance.getInstanceOperation().getOperation()
              .equals(InstanceConstants.InstanceOperation.UNKNOWN));
  private static final Function<List<InstanceConfig>, Boolean> ALL_MATCHES_ARE_UNKNOWN_OR_EVACUATE =
      (matchingInstances) -> matchingInstances.isEmpty() || matchingInstances.stream().allMatch(
          instance -> instance.getInstanceOperation().getOperation()
              .equals(InstanceConstants.InstanceOperation.UNKNOWN)
              || instance.getInstanceOperation().getOperation()
              .equals(InstanceConstants.InstanceOperation.EVACUATE));
  private static final Function<List<InstanceConfig>, Boolean> ANY_MATCH_ENABLE_OR_DISABLE =
      (matchingInstances) -> !matchingInstances.isEmpty() && matchingInstances.stream().anyMatch(
          instance -> instance.getInstanceOperation().getOperation()
              .equals(InstanceConstants.InstanceOperation.ENABLE) || instance.getInstanceOperation()
              .getOperation().equals(InstanceConstants.InstanceOperation.DISABLE));

  // Validator map for valid instance operation transitions <currentOperation>:<targetOperation>:<validator>
  private static final ImmutableMap<InstanceConstants.InstanceOperation, ImmutableMap<InstanceConstants.InstanceOperation, Function<List<InstanceConfig>, Boolean>>>
      validInstanceOperationTransitions =
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
   */
  public static void validateInstanceOperationTransition(ConfigAccessor configAccessor,
      String clusterName, InstanceConfig instanceConfig,
      InstanceConstants.InstanceOperation currentOperation,
      InstanceConstants.InstanceOperation targetOperation) {
    // Check if the current operation and target operation are in the valid transitions map
    if (!validInstanceOperationTransitions.containsKey(currentOperation)
        || !validInstanceOperationTransitions.get(currentOperation).containsKey(targetOperation)) {
      throw new HelixException(
          "Invalid instance operation transition from " + currentOperation + " to "
              + targetOperation);
    }

    // Throw exception if the validation fails
    if (!validInstanceOperationTransitions.get(currentOperation).get(targetOperation)
        .apply(findInstancesWithMatchingLogicalId(configAccessor, clusterName, instanceConfig))) {
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
   * Sets the instance operation for the given instance.
   *
   * @param configAccessor      The ConfigAccessor instance
   * @param baseAccessor        The BaseDataAccessor instance
   * @param clusterName         The cluster name
   * @param instanceName        The instance name
   * @param instanceOperation   The instance operation to set
   */
  public static void setInstanceOperation(ConfigAccessor configAccessor,
      BaseDataAccessor<ZNRecord> baseAccessor, String clusterName, String instanceName,
      InstanceConfig.InstanceOperation instanceOperation) {
    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);

    // Retrieve the current instance configuration
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new HelixException("Cluster " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }

    // Validate the instance operation transition
    validateInstanceOperationTransition(configAccessor, clusterName, instanceConfig,
        instanceConfig.getInstanceOperation().getOperation(),
        instanceOperation == null ? InstanceConstants.InstanceOperation.ENABLE
            : instanceOperation.getOperation());

    // Update the instance operation
    boolean succeeded = baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + clusterName + ", instance: " + instanceName
              + ", participant config is null");
        }

        InstanceConfig config = new InstanceConfig(currentData);
        config.setInstanceOperation(instanceOperation);
        return config.getRecord();
      }
    }, AccessOption.PERSISTENT);

    if (!succeeded) {
      throw new HelixException(
          "Failed to update instance operation. Please check if instance is disabled.");
    }
  }
}

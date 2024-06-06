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

import com.google.common.collect.ImmutableSet;
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
  private InstanceUtil() {
  }

  public static void validateInstanceOperationTransition(InstanceConfig matchingLogicalIdInstance,
      InstanceConstants.InstanceOperation currentOperation,
      InstanceConstants.InstanceOperation targetOperation) {
    boolean targetStateEnableOrDisable =
        targetOperation.equals(InstanceConstants.InstanceOperation.ENABLE)
            || targetOperation.equals(InstanceConstants.InstanceOperation.DISABLE);
    switch (currentOperation) {
      case ENABLE:
      case DISABLE:
        // ENABLE or DISABLE can be set to ENABLE, DISABLE, or EVACUATE at any time.
        if (ImmutableSet.of(InstanceConstants.InstanceOperation.ENABLE,
            InstanceConstants.InstanceOperation.DISABLE,
            InstanceConstants.InstanceOperation.EVACUATE).contains(targetOperation)) {
          return;
        }
      case SWAP_IN:
        // We can only ENABLE or DISABLE a SWAP_IN instance if there is an instance with matching logicalId
        // with an InstanceOperation set to UNKNOWN.
        if ((targetStateEnableOrDisable && (matchingLogicalIdInstance == null
            || matchingLogicalIdInstance.getInstanceOperation().getOperation()
            .equals(InstanceConstants.InstanceOperation.UNKNOWN))) || targetOperation.equals(
            InstanceConstants.InstanceOperation.UNKNOWN)) {
          return;
        }
      case EVACUATE:
        // EVACUATE can only be set to ENABLE or DISABLE when there is no instance with the same
        // logicalId in the cluster.
        if ((targetStateEnableOrDisable && matchingLogicalIdInstance == null)
            || targetOperation.equals(InstanceConstants.InstanceOperation.UNKNOWN)) {
          return;
        }
      case UNKNOWN:
        // UNKNOWN can be set to ENABLE or DISABLE when there is no instance with the same logicalId in the cluster
        // or the instance with the same logicalId in the cluster has InstanceOperation set to EVACUATE.
        // UNKNOWN can be set to SWAP_IN when there is an instance with the same logicalId in the cluster set to ENABLE,
        // or DISABLE.
        if ((targetStateEnableOrDisable && (matchingLogicalIdInstance == null
            || matchingLogicalIdInstance.getInstanceOperation().getOperation()
            .equals(InstanceConstants.InstanceOperation.EVACUATE)))) {
          return;
        } else if (targetOperation.equals(InstanceConstants.InstanceOperation.SWAP_IN)
            && matchingLogicalIdInstance != null && !ImmutableSet.of(
                InstanceConstants.InstanceOperation.UNKNOWN,
                InstanceConstants.InstanceOperation.EVACUATE)
            .contains(matchingLogicalIdInstance.getInstanceOperation().getOperation())) {
          return;
        }
      default:
        throw new HelixException(
            "InstanceOperation cannot be set to " + targetOperation + " when the instance is in "
                + currentOperation + " state");
    }
  }

  /**
   * Find the instance that the passed instance has a matching logicalId with.
   *
   * @param clusterName    The cluster name
   * @param instanceConfig The instance to find the matching instance for
   * @return The matching instance if found, null otherwise.
   */
  public static List<InstanceConfig> findInstancesMatchingLogicalId(ConfigAccessor configAccessor,
      String clusterName,
      InstanceConfig instanceConfig) {
    String logicalIdKey =
        ClusterTopologyConfig.createFromClusterConfig(configAccessor.getClusterConfig(clusterName))
            .getEndNodeType();
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
   * Set the instance operation for the given instance.
   *
   * @param clusterName       The cluster name
   * @param instanceName      The instance name
   * @param instanceOperation The instance operation to set
   */
  public static void setInstanceOperation(ConfigAccessor configAccessor,
      BaseDataAccessor<ZNRecord> baseAccessor, String clusterName, String instanceName,
      InstanceConfig.InstanceOperation instanceOperation) {
    String path = PropertyPathBuilder.instanceConfig(clusterName, instanceName);

    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new HelixException("Cluster " + clusterName + ", instance: " + instanceName
          + ", instance config does not exist");
    }
    List<InstanceConfig> matchingLogicalIdInstances =
        findInstancesMatchingLogicalId(configAccessor, clusterName, instanceConfig);
    validateInstanceOperationTransition(
        !matchingLogicalIdInstances.isEmpty() ? matchingLogicalIdInstances.get(0) : null,
        instanceConfig.getInstanceOperation().getOperation(),
        instanceOperation == null ? InstanceConstants.InstanceOperation.ENABLE
            : instanceOperation.getOperation());

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

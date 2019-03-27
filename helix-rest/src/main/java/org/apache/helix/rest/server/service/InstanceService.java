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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.helix.rest.server.json.instance.InstanceInfo;

import com.google.common.collect.ImmutableList;

public interface InstanceService {
  enum HealthCheck {
    /**
     * Check if instance is alive
     */
    INSTANCE_NOT_ALIVE,
    /**
     * Check if instance is enabled both in instance config and cluster config
     */
    INSTANCE_NOT_ENABLED,
    /**
     * Check if instance is stable
     * Stable means all the ideal state mapping matches external view (view of current state).
     */
    INSTANCE_NOT_STABLE,
    /**
     * Check if instance has 0 resource assigned
     */
    EMPTY_RESOURCE_ASSIGNMENT,
    /**
     * Check if instance has disabled partitions
     */
    HAS_DISABLED_PARTITION,
    /**
     * Check if instance has valid configuration (pre-requisite for all checks)
     */
    INVALID_CONFIG,
    /**
     * Check if instance has error partitions
     */
    HAS_ERROR_PARTITION;

    /**
     * Pre-defined list of checks to test if an instance can be stopped at runtime
     */
    public static List<HealthCheck> STOPPABLE_CHECK_LIST = Arrays.asList(HealthCheck.values());
    /**
     * Pre-defined list of checks to test if an instance is in healthy running state
     */
    public static List<HealthCheck> STARTED_AND_HEALTH_CHECK_LIST =
        ImmutableList.of(HealthCheck.INSTANCE_NOT_ALIVE, HealthCheck.INSTANCE_NOT_ENABLED,
            HealthCheck.INSTANCE_NOT_STABLE, HealthCheck.EMPTY_RESOURCE_ASSIGNMENT);
  }

  /**
   * Get the current instance stoppable checks based on Helix own business logic
   * @param clusterId
   * @param instanceName
   * @return a map where key is stoppable check name and boolean value represents whether the check
   *         succeeds
   */
  Map<String, Boolean> getInstanceHealthStatus(String clusterId, String instanceName,
      List<HealthCheck> healthChecks);

  /**
   * Get the overall status of the instance
   * @param clusterId
   * @param instanceName
   * @return
   */
  InstanceInfo getInstanceInfo(String clusterId, String instanceName,
      List<HealthCheck> healthChecks);
}

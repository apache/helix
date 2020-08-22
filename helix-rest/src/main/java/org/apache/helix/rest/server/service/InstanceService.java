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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;

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
        HAS_ERROR_PARTITION,
        /**
         * Check if all resources hosted on the instance can still meet the min active replica
         * constraint if this instance is shutdown
         */
        MIN_ACTIVE_REPLICA_CHECK_FAILED;

        /**
         * Pre-defined list of checks to test if an instance can be stopped at runtime
         */
        public static List<HealthCheck> STOPPABLE_CHECK_LIST = Arrays.asList(HealthCheck.values());
        /**
         * Pre-defined list of checks to test if an instance is in healthy running state
         */
        public static List<HealthCheck> STARTED_AND_HEALTH_CHECK_LIST = ImmutableList
            .of(INVALID_CONFIG, INSTANCE_NOT_ALIVE, INSTANCE_NOT_ENABLED, INSTANCE_NOT_STABLE,
                EMPTY_RESOURCE_ASSIGNMENT);
    }

    /**
     * Get the overall status of the instance
     *
     * @param clusterId    The cluster id
     * @param instanceName The instance name
     * @return An instance of {@link InstanceInfo} easily convertible to JSON
     */
    InstanceInfo getInstanceInfo(String clusterId, String instanceName,
                                 List<HealthCheck> healthChecks);

    /**
     * Get the current instance stoppable checks
     *
     * @param clusterId    The cluster id
     * @param instanceName The instance name
     * @param jsonContent  The json payloads from client side
     * @return An instance of {@link StoppableCheck} easily convertible to JSON
     * @throws IOException in case of network failure
     */
    StoppableCheck getInstanceStoppableCheck(String clusterId, String instanceName,
                                             String jsonContent) throws IOException;

    /**
     * Batch get StoppableCheck results for a list of instances in one cluster
     *
     * @param clusterId   The cluster id
     * @param instances   The list of instances
     * @param jsonContent The json payloads from client side
     * @return A map contains the instance as key and the StoppableCheck as the value
     * @throws IOException in case of network failure
     */
    Map<String, StoppableCheck> batchGetInstancesStoppableChecks(String clusterId, List<String> instances, String jsonContent)
            throws IOException;
}

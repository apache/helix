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
import java.util.List;
import java.util.Map;

import org.apache.helix.rest.clusterMaintenanceService.HealthCheck;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;

public interface InstanceService {
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

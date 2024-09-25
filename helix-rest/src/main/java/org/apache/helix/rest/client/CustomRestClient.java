package org.apache.helix.rest.client;

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
import java.util.Set;

/**
 * Interacting with participant side to query for its health checks
 */
public interface CustomRestClient {
  /**
   * Get stoppable check result on instance
   * @param baseUrl the base url of the participant
   * @param customPayloads generic payloads required from client side and helix only works as proxy
   * @return a map where key is custom stoppable check name and boolean value indicates if the check
   *         succeeds
   * @throws IOException
   */
  Map<String, Boolean> getInstanceStoppableCheck(String baseUrl, Map<String, String> customPayloads)
      throws IOException;

  /**
   * Get stoppable check result on a list of partitions on the instance
   *
   * @param baseUrl the base url of the participant
   * @param partitions a list of partitions maintained by the participant
   * @param customPayloads generic payloads required from client side and helix only works as proxy
   * @return a map where key is partition name and boolean value indicates if the partition is
   *         healthy
   * @throws IOException
   */
  Map<String, Boolean> getPartitionStoppableCheck(String baseUrl, List<String> partitions,
      Map<String, String> customPayloads) throws IOException;

  /**
   * Get the stoppable check result for each of the given instances from the aggregated health
   * check endpoint.
   * @param baseUrl the base url of the aggregated health status check endpoint
   * @param instances a list of instances to check
   * @param toBeStoppedInstances a set of instances which are assumed stopped
   * @param clusterId the cluster id
   * @param customPayloads generic payloads required from client side and helix only works as proxy
   * @return a map where key is instance name and value is a list of failed checks
   * @throws IOException
   */
  Map<String, List<String>> getAggregatedStoppableCheck(String baseUrl, List<String> instances,
      Set<String> toBeStoppedInstances, String clusterId, Map<String, String> customPayloads)
      throws IOException;
}

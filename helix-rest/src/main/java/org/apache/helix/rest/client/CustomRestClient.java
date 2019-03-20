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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;


/**
 * Interface for interacting with client side rest endpoints
 */
public interface CustomRestClient {
  /**
   * Get stoppable check result on instance
   *
   * @param customPayloads generic payloads required from client side and helix only works as proxy
   * @return a map where key is custom stoppable check name and boolean value indicates if the check succeeds
   */
  Map<String, Boolean> getInstanceStoppableCheck(Map<String, String> customPayloads);
  /**
   * Get stoppable check result on partition
   *
   * @param customPayloads generic payloads required from client side and helix only works as proxy
   * @return a map where key is custom stoppable check name and boolean value indicates if the check succeeds
  */
  Map<String, Boolean> getPartitionStoppableCheck(Map<String, String> customPayloads);
}

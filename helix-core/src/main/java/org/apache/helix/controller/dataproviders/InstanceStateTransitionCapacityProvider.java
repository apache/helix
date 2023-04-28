package org.apache.helix.controller.dataproviders;

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

import java.util.Map;


/**
 * An interface to provide capacity data for instance for state transition.
 * It should measure the level of concurrent state transition the instance can allow at the time of query, considering
 * the existing load and pending state transition. In other word, it should measure the dynamic "headroom".
 * The actual implementation could be stateful, and may change over time as load and pending transitions change.
 */
public interface InstanceStateTransitionCapacityProvider {

  /**
   * Get the instance remaining capacity for state transition in the form of key-value pairs.
   * This should account for any allocated resources and pending transitions, the returned capacity map is the headroom
   * for additional safe state transitions.
   * @param instanceName instance name to query
   * @return <str, int> capacity pairs of all defined attributes for the instance
   */
  Map<String, Integer> getInstanceCapacity(String instanceName);
}

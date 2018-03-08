package org.apache.helix.controller.rebalancer.constraint.dataprovider;

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

import org.apache.helix.api.rebalancer.constraint.dataprovider.CapacityProvider;

import java.util.HashMap;
import java.util.Map;

public class MockCapacityProvider implements CapacityProvider {
  private final int _defaultCapacity;
  private final Map<String, Integer> _capacityMap = new HashMap<>();
  private final Map<String, Integer> _usageMap = new HashMap<>();

  public MockCapacityProvider(Map<String, Integer> capacityMap, int defaultCapacity) {
    _capacityMap.putAll(capacityMap);
    _defaultCapacity = defaultCapacity;
  }

  @Override
  public int getParticipantCapacity(String participant) {
    if (_capacityMap.containsKey(participant)) {
      return _capacityMap.get(participant);
    }
    return _defaultCapacity;
  }

  @Override
  public int getParticipantProvisioned(String participant) {
    if (_usageMap.containsKey(participant)) {
      return _usageMap.get(participant);
    }
    return 0;
  }
}

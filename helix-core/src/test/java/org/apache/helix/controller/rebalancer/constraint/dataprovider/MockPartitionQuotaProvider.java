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

import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionQuotaProvider;

import java.util.HashMap;
import java.util.Map;

public class MockPartitionQuotaProvider implements PartitionQuotaProvider {
  private final int _defaultQuota;
  private Map<String, Map<String, Integer>> _partitionQuotaMap = new HashMap<>();

  public MockPartitionQuotaProvider(int defaultQuota) {
    // use the default quota
    _defaultQuota = defaultQuota;
  }

  public MockPartitionQuotaProvider(Map<String, Map<String, Integer>> partitionQuotaMap,
      int defaultQuota) {
    _partitionQuotaMap = partitionQuotaMap;
    _defaultQuota = defaultQuota;
  }

  @Override
  public int getPartitionQuota(String resource, String partition) {
    if (_partitionQuotaMap.containsKey(resource) && _partitionQuotaMap.get(resource)
        .containsKey(partition)) {
      return _partitionQuotaMap.get(resource).get(partition);
    }
    return _defaultQuota;
  }
}

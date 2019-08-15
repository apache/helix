package org.apache.helix.rest.common;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.client.CustomRestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper for {@link ZKHelixDataAccessor} that caches the result of the batch reads it
 * performs.
 * Note that the usage of this object is valid for one REST request.
 */
public class HelixDataAccessorWrapper extends ZKHelixDataAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(HelixDataAccessorWrapper.class);
  private static final ExecutorService POOL = Executors.newCachedThreadPool();

  static final String PARTITION_HEALTH_KEY = "PARTITION_HEALTH";
  static final String IS_HEALTHY_KEY = "IS_HEALTHY";
  static final String EXPIRY_KEY = "EXPIRE";

  private final Map<PropertyKey, HelixProperty> _propertyCache = new HashMap<>();
  private final Map<PropertyKey, List<String>> _batchNameCache = new HashMap<>();
  protected CustomRestClient _restClient;

  public HelixDataAccessorWrapper(ZKHelixDataAccessor dataAccessor) {
    super(dataAccessor);
    _restClient = CustomRestClientFactory.get();
  }

  public Map<String, Map<String, Boolean>> getAllPartitionsHealthOnLiveInstance(
      RESTConfig restConfig, Map<String, String> customPayLoads) {
    // Only checks the instances are online with valid reports
    List<String> liveInstances = getChildNames(keyBuilder().liveInstances());
    // Make a parallel batch call for getting all healthreports from ZK.
    List<HelixProperty> zkHealthReports = getProperty(liveInstances.stream()
        .map(instance -> keyBuilder().healthReport(instance, PARTITION_HEALTH_KEY))
        .collect(Collectors.toList()), false);
    Map<String, Future<Map<String, Boolean>>> parallelTasks = new HashMap<>();
    for (int i = 0; i < liveInstances.size(); i++) {
      String liveInstance = liveInstances.get(i);
      Optional<ZNRecord> maybeHealthRecord =
          Optional.ofNullable(zkHealthReports.get(i)).map(HelixProperty::getRecord);
      parallelTasks.put(liveInstance,
          POOL.submit(() -> maybeHealthRecord
              .map(record -> getPartitionsHealth(liveInstance, record, restConfig, customPayLoads))
              .orElseGet(() -> getHealthStatusFromRest(liveInstance, Collections.emptyList(),
                  restConfig, customPayLoads))));
    }

    Map<String, Map<String, Boolean>> result = new HashMap<>();

    for (Map.Entry<String, Future<Map<String, Boolean>>> instanceToFuturePartitionHealth : parallelTasks
        .entrySet()) {
      String instance = instanceToFuturePartitionHealth.getKey();
      try {
        result.put(instance, instanceToFuturePartitionHealth.getValue().get());
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Failed to get partition health for instance {}", instance, e);
        result.put(instance, Collections.emptyMap());
      }
    }

    return result;
  }

  private Map<String, Boolean> getPartitionsHealth(String instance, ZNRecord partitionHealthRecord,
      RESTConfig restConfig, Map<String, String> customPayLoads) {
    Map<String, Boolean> result = new HashMap<>();
    List<String> expiredPartitions = new ArrayList<>();
    for (String partitionName : partitionHealthRecord.getMapFields().keySet()) {
      Map<String, String> healthMap = partitionHealthRecord.getMapField(partitionName);
      if (healthMap == null
          || Long.parseLong(healthMap.get(EXPIRY_KEY)) < System.currentTimeMillis()) {
        // Clean all the existing checks. If we do not clean it, when we do the customized
        // check,
        // Helix may think these partitions are only partitions holding on the instance.
        // But it could potentially have some partitions are unhealthy for expired ones.
        // It could problem for shutting down instances.
        expiredPartitions.add(partitionName);
        continue;
      }

      result.put(partitionName, Boolean.valueOf(healthMap.get(IS_HEALTHY_KEY)));
    }
    if (!expiredPartitions.isEmpty()) {
      Map<String, Boolean> updatedHealthStatus =
          getHealthStatusFromRest(instance, expiredPartitions, restConfig, customPayLoads);
      result.putAll(updatedHealthStatus);
    }

    return result;
  }

  private Map<String, Boolean> getHealthStatusFromRest(String instance, List<String> partitions,
      RESTConfig restConfig, Map<String, String> customPayLoads) {
    try {
      return _restClient.getPartitionStoppableCheck(restConfig.getBaseUrl(instance), partitions,
          customPayLoads);
    } catch (IOException e) {
      LOG.error("Failed to get partition status on instance {}, partitions: {}", instance,
          partitions, e);
      return Collections.emptyMap();
    }
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    if (_propertyCache.containsKey(key)) {
      return (T) _propertyCache.get(key);
    }
    T property = super.getProperty(key);
    _propertyCache.put(key, property);
    return property;
  }

  @Override
  public List<String> getChildNames(PropertyKey key) {
    if (_batchNameCache.containsKey(key)) {
      return _batchNameCache.get(key);
    }

    List<String> names = super.getChildNames(key);
    _batchNameCache.put(key, names);

    return names;
  }
}

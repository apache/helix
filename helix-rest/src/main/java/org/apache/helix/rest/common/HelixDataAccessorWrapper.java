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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.client.CustomRestClientFactory;
import org.apache.helix.rest.common.datamodel.RestSnapShot;
import org.apache.helix.rest.server.service.InstanceService;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
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

  public static final String PARTITION_HEALTH_KEY = "PARTITION_HEALTH";
  public static final String IS_HEALTHY_KEY = "IS_HEALTHY";
  public static final String EXPIRY_KEY = "EXPIRE";

  // Metric names for custom partition check
  private static final String CUSTOM_PARTITION_CHECK_HTTP_REQUESTS_ERROR_TOTAL = MetricRegistry
      .name(InstanceService.class, "custom_partition_check_http_requests_error_total");
  private static final String CUSTOM_PARTITION_CHECK_HTTP_REQUESTS_DURATION =
      MetricRegistry.name(InstanceService.class, "custom_partition_check_http_requests_duration");

  protected String _namespace;
  protected CustomRestClient _restClient;

  private RestSnapShotSimpleImpl _restSnapShot;

  /**
   * @deprecated Because a namespace is required, please use the other constructors.
   *
   * @param dataAccessor Zk Helix data accessor used to access ZK.
   */
  @Deprecated
  public HelixDataAccessorWrapper(ZKHelixDataAccessor dataAccessor) {
    this(dataAccessor, CustomRestClientFactory.get(), HelixRestNamespace.DEFAULT_NAMESPACE_NAME);
  }

  @Deprecated
  public HelixDataAccessorWrapper(ZKHelixDataAccessor dataAccessor,
      CustomRestClient customRestClient) {
    this(dataAccessor, customRestClient, HelixRestNamespace.DEFAULT_NAMESPACE_NAME);
  }

  public HelixDataAccessorWrapper(ZKHelixDataAccessor dataAccessor,
      CustomRestClient customRestClient, String namespace) {
    super(dataAccessor);
    _restClient = customRestClient;
    _namespace = namespace;
    _restSnapShot = new RestSnapShotSimpleImpl(_clusterName);
  }

  public Map<String, Map<String, Boolean>> getAllPartitionsHealthOnLiveInstance(
      RESTConfig restConfig, Map<String, String> customPayLoads) {
    return getAllPartitionsHealthOnLiveInstance(restConfig, customPayLoads, false);
  }

  /**
   * Retrieve partition health status for each live instances combined with reading health partition report from ZK
   * and customized REST API call.
   *
   * @param restConfig        restConfig for the cluster contains customize REST API endpoint
   * @param customPayLoads    User passed in customized payloads
   * @param skipZKRead        Query the participant end point directly rather than fetch for
   *                          partition health from ZK if this flag is true.
   * @return                  A map of instance -> partition -> healthy or not (boolean).
   */
  public Map<String, Map<String, Boolean>> getAllPartitionsHealthOnLiveInstance(
      RESTConfig restConfig, Map<String, String> customPayLoads, boolean skipZKRead) {
    // Only checks the instances are online with valid reports
    List<String> liveInstances = getChildNames(keyBuilder().liveInstances());
    // Make a parallel batch call for getting all healthreports from ZK.
    List<HelixProperty> zkHealthReports;
    if (!skipZKRead) {
      zkHealthReports = getProperty(liveInstances.stream()
          .map(instance -> keyBuilder().healthReport(instance, PARTITION_HEALTH_KEY))
          .collect(Collectors.toList()), false);
    } else {
      zkHealthReports =
          liveInstances.stream().map(instance -> new HelixProperty(instance)).collect(Collectors.toList());
    }
    Map<String, Future<Map<String, Boolean>>> parallelTasks = new HashMap<>();
    for (int i = 0; i < liveInstances.size(); i++) {
      String liveInstance = liveInstances.get(i);
      Optional<ZNRecord> maybeHealthRecord = Optional.ofNullable(zkHealthReports.get(i)).map(HelixProperty::getRecord);
      parallelTasks.put(liveInstance, POOL.submit(() -> maybeHealthRecord.map(
          record -> getPartitionsHealthFromCustomAPI(liveInstance, record, restConfig, customPayLoads, skipZKRead))
          .orElseGet(
              () -> getHealthStatusFromRest(liveInstance, Collections.emptyList(), restConfig, customPayLoads))));
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

  /**
   * Get the partition health status from custom API. When we skip reading data from ZK, partitionHealthRecord will be
   * empty. We need a full refresh of all the partitions. If we pass the empty set of partition to be refresh, custom
   * API will return nothing.
   *
   * @param instance                instance to query
   * @param partitionHealthRecord   Retrieved partition health data from ZK. Could be emptry if we skip reading from ZK.
   * @param restConfig              restConfig for the cluster contains custom API endpoint
   * @param customPayLoads          User passed in customized payloads
   * @param requireFullRead         Get all the partition status from custom API endpoint if it is true. It should skip
   *                                the payload of "PARTITION : list of partition need to be fetch" in REST call.
   * @return                        A map of instance -> partition -> healthy or not (boolean).
   */
  private Map<String, Boolean> getPartitionsHealthFromCustomAPI(String instance, ZNRecord partitionHealthRecord,
      RESTConfig restConfig, Map<String, String> customPayLoads, boolean requireFullRead) {
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

    if (requireFullRead) {
      result.putAll(getHealthStatusFromRest(instance, null, restConfig, customPayLoads));
    } else if (!expiredPartitions.isEmpty()) {
      result.putAll(getHealthStatusFromRest(instance, expiredPartitions, restConfig, customPayLoads));
    }

    return result;
  }

  private Map<String, Boolean> getHealthStatusFromRest(String instance, List<String> partitions,
      RESTConfig restConfig, Map<String, String> customPayLoads) {
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(_namespace);
    // Total requests metric is included as an attribute(Count) in timers
    try (final Timer.Context timer = metrics.timer(CUSTOM_PARTITION_CHECK_HTTP_REQUESTS_DURATION)
        .time()) {
      return _restClient.getPartitionStoppableCheck(restConfig.getBaseUrl(instance), partitions,
          customPayLoads);
    } catch (IOException e) {
      LOG.error("Failed to get partition status on instance {}, partitions: {}", instance,
          partitions, e);
      metrics.counter(CUSTOM_PARTITION_CHECK_HTTP_REQUESTS_ERROR_TOTAL).inc();
      return Collections.emptyMap();
    }
  }

  public RestSnapShot getRestSnapShot() {
    return _restSnapShot;
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    T property = _restSnapShot.getProperty(key);
    if (property == null) {
      property = super.getProperty(key);
      _restSnapShot.updateValue(key, property);
    }

    return property;
  }

  @Override
  public List<String> getChildNames(PropertyKey key) {
    List<String> names = _restSnapShot.getChildNames(key);
    if (names == null) {
      names = super.getChildNames(key);
      _restSnapShot.updateChildNames(key, names);
    }
    return names;
  }

  public void fetchIdealStatesExternalViewStateModel() {
    PropertyKey.Builder propertyKeyBuilder = this.keyBuilder();
    List<String> resources = getChildNames(propertyKeyBuilder.idealStates());

    for (String resourceName : resources) {
      getProperty(propertyKeyBuilder.idealStates(resourceName));
      ExternalView externalView = getProperty(propertyKeyBuilder.externalView(resourceName));
      if (externalView != null) {
        String stateModeDef = externalView.getStateModelDefRef();
        getProperty(propertyKeyBuilder.stateModelDef(stateModeDef));
      }
    }
    _restSnapShot.addPropertyType(PropertyType.IDEALSTATES);
    _restSnapShot.addPropertyType(PropertyType.EXTERNALVIEW);
    _restSnapShot.addPropertyType(PropertyType.STATEMODELDEFS);
  }

  public void populateCache(List<PropertyType> propertyTypes) {
    for (PropertyType propertyType : propertyTypes) {
      switch (propertyType) {
        case IDEALSTATES:
        case EXTERNALVIEW:
        case STATEMODELDEFS: {
          if (!_restSnapShot.containsProperty(propertyType)) {
            fetchIdealStatesExternalViewStateModel();
          }
          break;
        }
        default:
          throw new UnsupportedOperationException("type selection is not supported yet!");
      }
    }
  }
}

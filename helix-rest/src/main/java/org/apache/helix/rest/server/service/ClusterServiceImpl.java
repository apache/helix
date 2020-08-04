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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.rest.server.json.cluster.ClusterInfo;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;

public class ClusterServiceImpl implements ClusterService {
  private final HelixDataAccessor _dataAccessor;
  private final ConfigAccessor _configAccessor;

  public ClusterServiceImpl(HelixDataAccessor dataAccessor, ConfigAccessor configAccessor) {
    _dataAccessor = dataAccessor;
    _configAccessor = configAccessor;
  }

  @Override
  public ClusterTopology getClusterTopology(String cluster) {
    String zoneField = _configAccessor.getClusterConfig(cluster).getFaultZoneType();
    PropertyKey.Builder keyBuilder = _dataAccessor.keyBuilder();
    List<InstanceConfig> instanceConfigs =
        _dataAccessor.getChildValues(keyBuilder.instanceConfigs(), true);
    Map<String, List<ClusterTopology.Instance>> instanceMapByZone = new HashMap<>();
    if (instanceConfigs != null && !instanceConfigs.isEmpty()) {
      for (InstanceConfig instanceConfig : instanceConfigs) {
        if (!instanceConfig.getDomainAsMap().containsKey(zoneField)) {
          continue;
        }
        final String instanceName = instanceConfig.getInstanceName();
        final ClusterTopology.Instance instance = new ClusterTopology.Instance(instanceName);
        final String zoneId = instanceConfig.getDomainAsMap().get(zoneField);
        if (instanceMapByZone.containsKey(zoneId)) {
          instanceMapByZone.get(zoneId).add(instance);
        } else {
          instanceMapByZone.put(zoneId, new ArrayList<ClusterTopology.Instance>() {
            {
              add(instance);
            }
          });
        }
      }
    }
    List<ClusterTopology.Zone> zones = new ArrayList<>();
    for (String zoneId : instanceMapByZone.keySet()) {
      ClusterTopology.Zone zone = new ClusterTopology.Zone(zoneId);
      zone.setInstances(instanceMapByZone.get(zoneId));
      zones.add(zone);
    }

    // Get all the instances names
    return new ClusterTopology(cluster, zones,
        instanceConfigs.stream().map(InstanceConfig::getInstanceName).collect(Collectors.toSet()));
  }

  @Override
  public ClusterInfo getClusterInfo(String clusterId) {
    ClusterInfo.Builder builder = new ClusterInfo.Builder(clusterId);
    PropertyKey.Builder keyBuilder = _dataAccessor.keyBuilder();
    LiveInstance controller =
        _dataAccessor.getProperty(_dataAccessor.keyBuilder().controllerLeader());
    if (controller != null) {
      builder.controller(controller.getInstanceName());
    } else {
      builder.controller("No Lead Controller");
    }

    return builder
        .paused(_dataAccessor.getBaseDataAccessor().exists(keyBuilder.pause().getPath(),
            AccessOption.PERSISTENT))
        .maintenance(_dataAccessor.getBaseDataAccessor().exists(keyBuilder.maintenance().getPath(),
            AccessOption.PERSISTENT))
        .idealStates(_dataAccessor.getChildNames(keyBuilder.idealStates()))
        .instances(_dataAccessor.getChildNames(keyBuilder.instances()))
        .liveInstances(_dataAccessor.getChildNames(keyBuilder.liveInstances())).build();
  }
}

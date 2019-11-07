package org.apache.helix.rest.server.service;

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
        _dataAccessor.getChildValues(keyBuilder.instanceConfigs());
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

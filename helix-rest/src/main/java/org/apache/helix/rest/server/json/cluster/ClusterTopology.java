package org.apache.helix.rest.server.json.cluster;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO class that can be easily convert to JSON object
 * The Cluster Topology represents the hierarchy of the cluster:
 * Cluster
 * - Zone
 * -- Rack(Optional)
 * --- Instance
 * Each layer consists its id and metadata
 */
public class ClusterTopology {
  @JsonProperty("id")
  private final String clusterId;
  @JsonProperty("zones")
  private List<Zone> zones;
  @JsonProperty("allInstances")
  private Set<String> allInstances;


  public ClusterTopology(String clusterId, List<Zone> zones, Set<String> allInstances) {
    this.clusterId = clusterId;
    this.zones = zones;
    this.allInstances = allInstances;
  }

  public String getClusterId() {
    return clusterId;
  }

  public List<Zone> getZones() {
    return zones;
  }

  public Set<String> getAllInstances() {
    return allInstances;
  }

  public static final class Zone {
    @JsonProperty("id")
    private final String id;
    @JsonProperty("instances")
    private List<Instance> instances;

    public Zone(String id) {
      this.id = id;
    }

    public Zone(String id, List<Instance> instances) {
      this.id = id;
      this.instances = instances;
    }

    public List<Instance> getInstances() {
      return instances;
    }

    public void setInstances(List<Instance> instances) {
      this.instances = instances;
    }

    public String getId() {
      return id;
    }
  }

  public static final class Instance {
    @JsonProperty("id")
    private final String id;

    public Instance(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }
  }

  public Map<String, Set<String>> toZoneMapping() {
    Map<String, Set<String>> zoneMapping = new HashMap<>();
    if (zones == null) {
      return Collections.emptyMap();
    }
    for (ClusterTopology.Zone zone : zones) {
      zoneMapping.put(zone.getId(), new HashSet<String>());
      if (zone.getInstances() != null) {
        for (ClusterTopology.Instance instance : zone.getInstances()) {
          zoneMapping.get(zone.getId()).add(instance.getId());
        }
      }
    }
    return zoneMapping;
  }
}

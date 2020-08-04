package org.apache.helix.rest.server.json.cluster;

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

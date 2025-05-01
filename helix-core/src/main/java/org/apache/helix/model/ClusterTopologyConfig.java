package org.apache.helix.model;

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

import java.util.LinkedHashMap;
import org.apache.helix.HelixException;
import org.apache.helix.controller.rebalancer.topology.Topology;

public class ClusterTopologyConfig {
  private static final String DEFAULT_DOMAIN_PREFIX = "Helix_default_";
  private static final String TOPOLOGY_SPLITTER = "/";

  private final boolean _topologyAwareEnabled;
  private final String _endNodeType;
  private final String _faultZoneType;
  private final LinkedHashMap<String, String> _topologyKeyDefaultValue;

  private ClusterTopologyConfig(boolean topologyAwareEnabled, String endNodeType, String faultZoneType,
      LinkedHashMap<String, String> topologyKeyDefaultValue) {
    _topologyAwareEnabled = topologyAwareEnabled;
    _endNodeType = endNodeType;
    _faultZoneType = faultZoneType;
    _topologyKeyDefaultValue = topologyKeyDefaultValue;
  }

  /**
   * Populate faultZone, endNodetype and and a LinkedHashMap containing pathKeys default values for
   * clusterConfig.Topology. The LinkedHashMap will be empty if clusterConfig.Topology is unset.
   *
   * @return an instance of {@link ClusterTopologyConfig}
   */
  public static ClusterTopologyConfig createFromClusterConfig(ClusterConfig clusterConfig) {
    if (!clusterConfig.isTopologyAwareEnabled()) {
      return new ClusterTopologyConfig(
          false,
          Topology.Types.INSTANCE.name(),
          Topology.Types.INSTANCE.name(),
          new LinkedHashMap<>());
    }
    // Assign default cluster topology definition, i,e. /root/zone/instance
    String endNodeType = Topology.Types.INSTANCE.name();
    String faultZoneType = Topology.Types.ZONE.name();
    LinkedHashMap<String, String> topologyKeyDefaultValue = new LinkedHashMap<>();

    String topologyDef = clusterConfig.getTopology();
    if (topologyDef != null) {
      for (String topologyKey : topologyDef.trim().split(TOPOLOGY_SPLITTER)) {
        if (!topologyKey.isEmpty()) {
          topologyKeyDefaultValue.put(topologyKey, DEFAULT_DOMAIN_PREFIX + topologyKey);
          endNodeType = topologyKey;
        }
      }
      if (topologyKeyDefaultValue.isEmpty()) {
        throw new IllegalArgumentException("Invalid cluster topology definition " + topologyDef);
      }
      faultZoneType = clusterConfig.getFaultZoneType();
      if (faultZoneType == null) {
        faultZoneType = endNodeType;
      } else if (!topologyKeyDefaultValue.containsKey(faultZoneType)) {
        throw new HelixException(
            String.format("Invalid fault zone type %s, not present in topology definition %s.",
                faultZoneType, clusterConfig.getTopology()));
      }
    }
    return new ClusterTopologyConfig(true, endNodeType, faultZoneType, topologyKeyDefaultValue);
  }

  /**
   * Replace the fault zone in the given topology string with the new fault zone.
   *
   * @param topologyString the topology string to be modified
   * @param newFaultZone the new fault zone to be set
   * @return the modified topology string
   */
  public String replaceFaultZoneInTopologyString(String topologyString, String newFaultZone) {
    if (topologyString == null || topologyString.isEmpty()) {
      throw new IllegalArgumentException("Topology string cannot be null or empty");
    }
    String[] newTopologyString = topologyString.split(TOPOLOGY_SPLITTER);
    for (int i = 0; i < newTopologyString.length; i++) {
      if (newTopologyString[i].equals(_faultZoneType)) {
        newTopologyString[i] = newFaultZone;
      }
    }
    return String.join(TOPOLOGY_SPLITTER, newTopologyString);
  }

  public boolean isTopologyAwareEnabled() {
    return _topologyAwareEnabled;
  }

  public String getEndNodeType() {
    return _endNodeType;
  }

  public String getFaultZoneType() {
    return _faultZoneType;
  }

  public LinkedHashMap<String, String> getTopologyKeyDefaultValue() {
    return _topologyKeyDefaultValue;
  }
}

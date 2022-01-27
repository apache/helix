package org.apache.helix.cloud.virtualTopologyGroup;

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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class AssignmentEvaluation {
  private final Map<String, Set<String>> _assignment;
  private final Map<String, Set<String>> _coveredZonesByVirtualGroup;
  private final Map<String, Set<String>> _affectedVirtualGroupByZone;
  private final Map<String, String> _zoneByInstance;
  private final Map<String, String> _virtualGroupByInstance;

  public AssignmentEvaluation(Map<String, Set<String>> assignment, Map<String, Set<String>> zoneMapping) {
    _assignment = assignment;

    _zoneByInstance = new HashMap<>();
    _virtualGroupByInstance = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : zoneMapping.entrySet()) {
      entry.getValue().forEach(instance -> _zoneByInstance.put(instance, entry.getKey()));
    }
    // key is virtual group, value is the set of physical zones that the virtual group span across
    _coveredZonesByVirtualGroup = new HashMap<>();
    // key is physical zone, value is the set of affected virtual group if the physical zone is down
    _affectedVirtualGroupByZone = new HashMap<>();
    for (String virtualGroup : assignment.keySet()) {
      _coveredZonesByVirtualGroup.put(virtualGroup, computeCoveredZones(_zoneByInstance, assignment.get(virtualGroup)));
      for (String instance : assignment.get(virtualGroup)) {
        String zone = _zoneByInstance.get(instance);
        _virtualGroupByInstance.put(instance, virtualGroup);
        _affectedVirtualGroupByZone.putIfAbsent(zone, new HashSet<>());
        _affectedVirtualGroupByZone.get(zone).add(virtualGroup);
      }
    }
  }

  public void print() {
    for (Map.Entry<String, Set<String>> entry : _coveredZonesByVirtualGroup.entrySet()) {
      System.out.println("virtual group: " + entry.getKey() + ", " + "covered physical zones: " + entry.getValue());
    }
    for (Map.Entry<String, Set<String>> entry : _affectedVirtualGroupByZone.entrySet()) {
      System.out.println("physical zone " + entry.getKey() + " impacts " + entry.getValue());
    }
    System.out.println(generateStats().toString());
  }

  /**
   * Compare with a given virtual group assignment and compute the number of instances that are assigned a different group.
   * @param that an instance of {@link AssignmentEvaluation} to compare
   * @return the number of instances changing virtual group
   */
  public int compareAssignments(AssignmentEvaluation that) {
    int diff = 0;
    for (String instance : _zoneByInstance.keySet()) {
      if (!that._zoneByInstance.containsKey(instance)) {
        continue;
      }
      if (!that._virtualGroupByInstance.get(instance).equals(_virtualGroupByInstance.get(instance))) {
        diff++;
      }
    }
    return diff;
  }

  private Stats generateStats() {
    Stats stats = new Stats();
    stats.maxCoveredZones = getMaxValuesSize(_coveredZonesByVirtualGroup);
    stats.rangeCoveredZones = stats.maxCoveredZones - getMinValuesSize(_coveredZonesByVirtualGroup);
    stats.maxImpactVirtualZones = getMaxValuesSize(_affectedVirtualGroupByZone);
    stats.rangeImpactVirtualZones = stats.maxImpactVirtualZones - getMinValuesSize(_affectedVirtualGroupByZone);
    stats.rangeGroupInstances = getMaxValuesSize(_assignment) - getMinValuesSize(_assignment);
    return stats;
  }

  private static int getMaxValuesSize(Map<String, Set<String>> map) {
    return map.values().stream().map(Collection::size).max(Integer::compare).get();
  }

  private static int getMinValuesSize(Map<String, Set<String>> map) {
    return map.values().stream().map(Collection::size).min(Integer::compare).get();
  }

  private static Set<String> computeCoveredZones(Map<String, String> zoneByInstance, Set<String> instances) {
    return instances.stream()
        .map(zoneByInstance::get)
        .collect(Collectors.toSet());
  }

  static class Stats {
    int maxCoveredZones;
    int rangeCoveredZones;
    int maxImpactVirtualZones;
    int rangeImpactVirtualZones;
    int rangeGroupInstances;

    @Override
    public String toString() {
      return "{" + "maxCoveredZones=" + maxCoveredZones + ", rangeCoveredZones=" + rangeCoveredZones
          + ", maxImpactVirtualZones=" + maxImpactVirtualZones + ", rangeImpactVirtualZones=" + rangeImpactVirtualZones
          + ", rangeGroupInstances=" + rangeGroupInstances + '}';
    }
  }
}

package org.apache.helix.cloud.topology;

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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.math3.util.Pair;

import static org.apache.helix.util.VirtualTopologyUtil.computeVirtualGroupId;

/**
 * A virtual group assignment algorithm that assigns zones and their instances to virtual groups
 * a way that preserves existing zone-to-group assignments whenever possible, and balances any
 * remaining unassigned zones across the least-loaded groups. If the requested number of groups
 * differs from the existing assignment, a new distribution is computed. Otherwise, if a zone
 * already exists in the provided assignment, all its instances (including newly discovered ones)
 * are placed in the same group, ensuring no zone is split across multiple virtual groups.
 */
public class FaultZoneBasedVirtualGroupAssignmentAlgorithm implements VirtualGroupAssignmentAlgorithm {

  private static final FaultZoneBasedVirtualGroupAssignmentAlgorithm _instance =
      new FaultZoneBasedVirtualGroupAssignmentAlgorithm();

  private FaultZoneBasedVirtualGroupAssignmentAlgorithm() {
  }

  public static FaultZoneBasedVirtualGroupAssignmentAlgorithm getInstance() {
    return _instance;
  }

  @Override
  public Map<String, Set<String>> computeAssignment(int numGroups, String virtualGroupName,
      Map<String, Set<String>> zoneMapping, Map<String, Set<String>> virtualGroupToInstancesMap) {
    // 1. If the number of requested virtual groups differs from the current assignment size,
    //    we must do a fresh assignment (the existing distribution is invalid).
    if (numGroups != virtualGroupToInstancesMap.size()) {
      Map<String, Set<String>> newAssignment = new HashMap<>();
      for (int i = 0; i < numGroups; i++) {
        newAssignment.put(computeVirtualGroupId(i, virtualGroupName), new HashSet<>());
      }

      // Assign all zones from scratch in a balanced manner.
      distributeUnassignedZones(newAssignment, new ArrayList<>(zoneMapping.keySet()), zoneMapping);
      return constructResult(newAssignment, zoneMapping);
    }

    // 2. Find unassigned zones. If there is any, incrementally assign them to the least-loaded
    //    virtual group.
    // Build instance-to-zone mapping for quick zone lookups.
    Map<String, String> instanceToZoneMapping = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : zoneMapping.entrySet()) {
      for (String instance : entry.getValue()) {
        instanceToZoneMapping.put(instance, entry.getKey());
      }
    }

    // Copy zoneMapping for tracking which zones are unassigned.
    Set<String> unassignedZones = new HashSet<>(zoneMapping.keySet());

    // Build virtual group -> zone mapping and remove assigned zones from the unassigned list
    Map<String, Set<String>> virtualGroupToZoneMapping = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : virtualGroupToInstancesMap.entrySet()) {
      virtualGroupToZoneMapping.putIfAbsent(entry.getKey(), new HashSet<>());
      for (String instance : entry.getValue()) {
        String zone = instanceToZoneMapping.get(instance);
        virtualGroupToZoneMapping.get(entry.getKey()).add(zone);
        unassignedZones.remove(zone);
      }
    }

    // If there are no unassigned zones, return the result as is.
    if (unassignedZones.isEmpty()) {
      return constructResult(virtualGroupToZoneMapping, zoneMapping);
    }

    // Distribute unassigned zones to keep the overall distribution balanced.
    distributeUnassignedZones(virtualGroupToZoneMapping, new ArrayList<>(unassignedZones),
        zoneMapping);
    return constructResult(virtualGroupToZoneMapping, zoneMapping);
  }

  @Override
  public Map<String, Set<String>> computeAssignment(int numGroups, String virtualGroupName,
      Map<String, Set<String>> zoneMapping) {
    return computeAssignment(numGroups, virtualGroupName, zoneMapping, new HashMap<>());
  }

  /**
   * Distributes unassigned zones across virtual groups in a balanced manner.
   * Assigns heavier zones first to the current least-loaded group.
   *
   * @param virtualGroupToZoneMapping Current assignment of virtual group -> set of zones.
   * @param unassignedZones            List of zones that have not been assigned to any group.
   * @param zoneMapping                Mapping of physical zone -> set of instances.
   */
  private void distributeUnassignedZones(
      Map<String, Set<String>> virtualGroupToZoneMapping, List<String> unassignedZones,
      Map<String, Set<String>> zoneMapping) {

    // Priority queue sorted by current load of the virtual group
    // We always assign new zones to the group with the smallest load to keep them balanced.
    Queue<String> minHeap = new PriorityQueue<>(
        Comparator.comparingInt(vg ->
            virtualGroupToZoneMapping.get(vg).stream()
                .map(zoneMapping::get)
                .mapToInt(Set::size)
                .sum()
        )
    );
    // Seed the min-heap with existing groups
    minHeap.addAll(virtualGroupToZoneMapping.keySet());

    // Sort unassigned zones by descending number of unassigned instances, assigning "heavier" zones first.
    unassignedZones.sort(Comparator.comparingInt(zone -> zoneMapping.get(zone).size())
        .reversed());

    // Assign each zone to the least-loaded group
    for (String zone : unassignedZones) {
      String leastLoadVg = minHeap.poll();
      virtualGroupToZoneMapping.get(leastLoadVg).add(zone);
      minHeap.offer(leastLoadVg);
    }
  }

  /**
   * Constructs the final result by mapping virtual groups to their instances.
   *
   * @param vgToZonesMapping    Mapping of virtual group -> set of zones.
   * @param zoneToInstancesMapping Mapping of zone -> set of instances.
   * @return Mapping of virtual group -> set of instances.
   */
  private Map<String, Set<String>> constructResult(Map<String, Set<String>> vgToZonesMapping,
      Map<String, Set<String>> zoneToInstancesMapping) {
    Map<String, Set<String>> result = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : vgToZonesMapping.entrySet()) {
      Set<String> instances = new HashSet<>();
      for (String zone : entry.getValue()) {
        instances.addAll(zoneToInstancesMapping.get(zone));
      }
      result.put(entry.getKey(), instances);
    }
    return result;
  }
}

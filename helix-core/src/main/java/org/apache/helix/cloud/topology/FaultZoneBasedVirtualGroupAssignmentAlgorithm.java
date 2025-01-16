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
      Map<String, Set<String>> zoneMapping, Map<String, Set<String>> virtualZoneMapping) {
    // Build instance-to-zone mapping for quick zone lookups.
    Map<String, String> instanceToZoneMapping = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : zoneMapping.entrySet()) {
      for (String instance : entry.getValue()) {
        instanceToZoneMapping.put(instance, entry.getKey());
      }
    }
    // Collect all instances from zoneMapping
    Set<String> allInstances = zoneMapping.values()
        .stream()
        .flatMap(Set::stream)
        .collect(Collectors.toSet());

    // 1. If the number of requested virtual groups differs from the current assignment size,
    //    we must do a fresh assignment (the existing distribution is invalid).
    if (numGroups != virtualZoneMapping.size()) {
      Map<String, Set<String>> newAssignment = new HashMap<>();
      for (int i = 0; i < numGroups; i++) {
        newAssignment.put(computeVirtualGroupId(i, virtualGroupName), new HashSet<>());
      }

      // Assign all instances from scratch in a balanced manner.
      return distributeUnassignedZones(newAssignment, allInstances, instanceToZoneMapping);
    }

    // Build a deep copy of the current assignment to avoid mutating it directly.
    Map<String, Set<String>> updatedAssignment = deepCopy(virtualZoneMapping);

    // 2. Identify unassigned instances.
    // Based on the existing assignment, build zone -> virtual group mapping for quick reference.
    Map<String, String> zoneToVirtualGroupMapping = new HashMap<>();
    Set<String> assignedInstances = new HashSet<>();
    for (Map.Entry<String, Set<String>> entry : updatedAssignment.entrySet()) {
      for (String instance : entry.getValue()) {
        String zone = instanceToZoneMapping.get(instance);
        assignedInstances.add(instance);
        zoneToVirtualGroupMapping.put(zone, entry.getKey());
      }
    }
    Set<String> unassigned = new HashSet<>(allInstances);
    unassigned.removeAll(assignedInstances);

    // 3. Attempt to place unassigned instances in existing virtual groups if their zones are
    //    already present in the assignment.
    Iterator<String> iterator = unassigned.iterator();
    while (iterator.hasNext()) {
      String instance = iterator.next();
      String zone = instanceToZoneMapping.get(instance);
      if (zoneToVirtualGroupMapping.containsKey(zone)) {
        updatedAssignment.get(zoneToVirtualGroupMapping.get(zone)).add(instance);
        iterator.remove();
      }
    }

    // 4. If unassigned instances remain, do an incremental assignment computation while not
    //    shuffling the existing assignment
    if (!unassigned.isEmpty()) {
      return distributeUnassignedZones(updatedAssignment, unassigned, instanceToZoneMapping);
    }

    return updatedAssignment;
  }

  /**
   * Distributes unassigned zones (and all their instances) across virtual groups in a balanced
   * manner. Each zone is treated as an indivisible unit, so instances from the same zone
   * always end up in the same virtual group. This ensures that no zone is split across multiple
   * groups.
   *
   * @param virtualZoneMapping     Current or partially completed assignment.
   * @param unassigned             The set of unassigned instances needing distribution.
   * @param instanceToZoneMapping  A quick-lookup map of instance -> zone.
   * @return                       Updated assignment of virtual group -> set of instances.
   */
  private Map<String, Set<String>> distributeUnassignedZones(
      Map<String, Set<String>> virtualZoneMapping, Set<String> unassigned,
      Map<String, String> instanceToZoneMapping) {

    // Create a deep copy so that we do not mutate the original mapping.
    Map<String, Set<String>> assignment = deepCopy(virtualZoneMapping);

    // Min-heap for load balancing: (virtualGroupId, currentLoad), sorted by currentLoad.
    // We always assign new zones to the group with the smallest load to keep them balanced.
    Queue<Pair<String, Integer>> minHeap =
        new PriorityQueue<>(Comparator.comparingInt(Pair::getValue));
    for (Map.Entry<String, Set<String>> entry : assignment.entrySet()) {
      minHeap.add(new Pair<>(entry.getKey(), entry.getValue().size()));
    }

    // Create unassigned zone -> instances mapping for chunk assignment.
    Map<String, Set<String>> unassignedZoneToInstances = new HashMap<>();
    for (String instance : unassigned) {
      unassignedZoneToInstances
          .computeIfAbsent(instanceToZoneMapping.get(instance), k -> new HashSet<>())
          .add(instance);
    }

    // Sort zones by descending number of unassigned instances, assigning "heavier" zones first.
    List<String> sortedUnassignedZones = new ArrayList<>(unassignedZoneToInstances.keySet());
    sortedUnassignedZones.sort((zone1, zone2) -> {
      int size1 = unassignedZoneToInstances.get(zone1).size();
      int size2 = unassignedZoneToInstances.get(zone2).size();
      return Integer.compare(size2, size1);
    });

    // Assign each unassigned zone to the current least-loaded virtual group in the heap.
    for (String zone : sortedUnassignedZones) {
      Pair<String, Integer> virtualGroupLoadPair = minHeap.poll();
      String virtualGroupId = virtualGroupLoadPair.getKey();
      assignment.get(virtualGroupId).addAll(unassignedZoneToInstances.get(zone));

      // Update the load and push the pair back to the min-heap.
      int newLoad = virtualGroupLoadPair.getValue() + unassignedZoneToInstances.get(zone).size();
      minHeap.offer(new Pair<>(virtualGroupId, newLoad));
    }

    return assignment;
  }

  /**
   * Creates a deep copy of a map of virtual group -> set of instances,
   *
   * @param virtualZoneMapping Original map to copy.
   * @return A fully independent copy of the given map.
   */
  private Map<String, Set<String>> deepCopy(Map<String, Set<String>> virtualZoneMapping) {
    Map<String, Set<String>> copy = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : virtualZoneMapping.entrySet()) {
      copy.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }
    return copy;
  }
}

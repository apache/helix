package org.apache.helix.controller.rebalancer.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;


public class ResourceUsageCalculator {
  /**
   * A convenient tool for calculating partition capacity usage based on the assignment and resource
   * weight provider.
   *
   * @param resourceAssignment
   * @param weightProvider
   * @return
   */
  public static Map<String, Integer> getResourceUsage(ResourcesStateMap resourceAssignment,
      PartitionWeightProvider weightProvider) {
    Map<String, Integer> newParticipantUsage = new HashMap<>();
    for (String resource : resourceAssignment.resourceSet()) {
      Map<Partition, Map<String, String>> stateMap =
          resourceAssignment.getPartitionStateMap(resource).getStateMap();
      for (Partition partition : stateMap.keySet()) {
        for (String participant : stateMap.get(partition).keySet()) {
          if (!newParticipantUsage.containsKey(participant)) {
            newParticipantUsage.put(participant, 0);
          }
          newParticipantUsage.put(participant, newParticipantUsage.get(participant) + weightProvider
              .getPartitionWeight(resource, partition.getPartitionName()));
        }
      }
    }
    return newParticipantUsage;
  }

  /**
   * Count total number of all partitions in the collection of resource assignments.
   * @param assignments collection of resource assignments
   * @return total number of all partitions in the collection of resource assignments.
   */
  public static int countResourceAssignmentPartitions(Collection<ResourceAssignment> assignments) {
    return assignments.stream()
        .mapToInt(resourceAssignment -> resourceAssignment.getMappedPartitions().size())
        .sum();
  }

  /**
   * Measure baseline divergence between baseline assignment and best possible assignment at
   * partition level. Baseline divergence = (total number of common partitions for each key)
   * / (total number of different partitions for each key)
   * @param baseline baseline assignment
   * @param bestPossibleAssignment best possible assignment
   * @return double value range at [0.0, 1.0]
   */
  public static double measureBaselineDivergence(Map<String, ResourceAssignment> baseline,
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    MapDifference<String, ResourceAssignment> mapDifference =
        Maps.difference(baseline, bestPossibleAssignment);
    Map<String, ResourceAssignment> matchedAssignment = mapDifference.entriesInCommon();
    Map<String, MapDifference.ValueDifference<ResourceAssignment>> differentAssignment =
        mapDifference.entriesDiffering();

    // For each matched assignment, count all partitions and sum up.
    int numMatchedPartitions = countResourceAssignmentPartitions(matchedAssignment.values());

    // Count total partitions.
    // 1. Add all partitions in each matched resource assignment.
    int numTotalPartitions = numMatchedPartitions;
    // 2. Add all partitions for each key that only exists in beseline.
    numTotalPartitions +=
        countResourceAssignmentPartitions(mapDifference.entriesOnlyOnLeft().values());
    // 3. Add all partitions for each key that only exists in best possible.
    numTotalPartitions +=
        countResourceAssignmentPartitions(mapDifference.entriesOnlyOnRight().values());

    // 4. Add all different partitions for both resource assignments in each entry.
    for (Map.Entry<String, MapDifference.ValueDifference<ResourceAssignment>> entry
        : differentAssignment.entrySet()) {
      ResourceAssignment left = entry.getValue().leftValue();
      ResourceAssignment right = entry.getValue().rightValue();
      Set<Partition> partitionUnion = Sets.union(Sets.newHashSet(left.getMappedPartitions()),
          Sets.newHashSet(right.getMappedPartitions()));
      numTotalPartitions += partitionUnion.size();
    }

    return numTotalPartitions == 0 ? 0.0d
        : (double) numMatchedPartitions / (double) numTotalPartitions;
  }
}

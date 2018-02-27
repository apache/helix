package org.apache.helix.controller.rebalancer.util;

import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.model.Partition;

import java.util.HashMap;
import java.util.Map;

public class ResourceUsageCalculator {
  /**
   * A convenient tool for calculating partition capacity usage based on the assignment and resource weight provider.
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
}

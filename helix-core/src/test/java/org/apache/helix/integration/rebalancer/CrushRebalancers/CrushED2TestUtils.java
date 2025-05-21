package org.apache.helix.integration.rebalancer.CrushRebalancers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CrushED2TestUtils {
  public static Map<Integer, List<Double>> getAvgPartitionsPerZoneType(
      Map<String, List<String>> preferenceList,
      Map<String, Integer> instanceToZoneType
  ) {
    Map<String, Integer> instancePartitionCount = countPartitionsPerInstance(preferenceList);
    Map<Integer, List<Integer>> zoneTypeToPartitionCounts =
        groupPartitionCountsByZone(instancePartitionCount, instanceToZoneType);
    return computeAveragePerZoneType(zoneTypeToPartitionCounts);
  }

  private static Map<String, Integer> countPartitionsPerInstance(Map<String, List<String>> preferenceList) {
    Map<String, Integer> instancePartitionCount = new HashMap<>();
    preferenceList.values().forEach(instances ->
        instances.forEach(instance ->
            instancePartitionCount.merge(instance, 1, Integer::sum)
        )
    );
    return instancePartitionCount;
  }

  private static Map<Integer, List<Integer>> groupPartitionCountsByZone(
      Map<String, Integer> instancePartitionCount,
      Map<String, Integer> instanceToZoneType
  ) {
    Map<Integer, List<Integer>> zoneTypeToPartitionCounts = new HashMap<>();
    instanceToZoneType.forEach((instance, zoneType) -> {
      int count = instancePartitionCount.getOrDefault(instance, 0);
      zoneTypeToPartitionCounts
          .computeIfAbsent(zoneType, z -> new ArrayList<>())
          .add(count);
    });
    return zoneTypeToPartitionCounts;
  }

  private static Map<Integer, List<Double>> computeAveragePerZoneType(Map<Integer, List<Integer>> zoneToCounts) {
    Map<Integer, List<Double>> avgPerZone = new HashMap<>();
    final double[] totalPartitions = {0.0};
    final int[] totalInstances = {0};

    // Pass 1: calculate average per zone + accumulate global sums
    zoneToCounts.forEach((zoneType, counts) -> {
      double sum = counts.stream().mapToInt(i -> i).sum();
      int instanceCount = counts.size();
      double avg = instanceCount != 0 ? sum / instanceCount : 0.0;

      avgPerZone.put(zoneType, new ArrayList<>(List.of(avg)));  // store only avg for now

      totalPartitions[0] += sum;
      totalInstances[0] += instanceCount;
    });

    // Compute ideal (global) average
    double idealAvg = totalInstances[0] != 0 ? totalPartitions[0] / totalInstances[0] : 1.0;

    // Pass 2: calculate skew vs. ideal and add to avgPerZone
    avgPerZone.forEach((zoneType, list) -> {
      double avg = list.get(0);
      double skew = idealAvg != 0.0 ? avg / idealAvg : Double.POSITIVE_INFINITY;
      list.add(skew);
      list.add(idealAvg);
    });

    return avgPerZone;
  }
}

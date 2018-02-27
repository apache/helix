package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;

import java.util.*;

public class CardDealingAdjustmentAlgorithm {
  private static int MAX_ADJUSTMENT = 2;

  private int _replica;
  // Instance -> FaultZone Tag
  private Map<String, String> _instanceFaultZone = new HashMap<>();
  private Map<String, Long> _instanceWeight = new HashMap<>();
  private long _totalWeight = 0;
  private Map<String, Long> _faultZoneWeight = new HashMap<>();
  // Record existing partitions that are assigned to a fault zone
  private Map<String, Set<String>> _faultZonePartitionMap = new HashMap<>();

  public CardDealingAdjustmentAlgorithm(Topology topology, int replica) {
    _replica = replica;
    // Get all instance related information.
    for (Node zone : topology.getFaultZones()) {
      _faultZoneWeight.put(zone.getName(), zone.getWeight());
      if (!_faultZonePartitionMap.containsKey(zone.getName())) {
        _faultZonePartitionMap.put(zone.getName(), new HashSet<String>());
      }
      for (Node instance : Topology.getAllLeafNodes(zone)) {
        if (!instance.isFailed()) {
          _instanceWeight.put(instance.getName(), instance.getWeight());
          _totalWeight += instance.getWeight();
          _instanceFaultZone.put(instance.getName(), zone.getName());
        }
      }
    }
  }

  public boolean computeMapping(Map<String, List<String>> nodeToPartitionMap, int randomSeed) {
    // Records exceed partitions
    TreeMap<String, Integer> toBeReassigned = new TreeMap<>();

    // Calculate total partitions that need to be calculated
    long totalReplicaCount = 0;
    for (List<String> partitions : nodeToPartitionMap.values()) {
      totalReplicaCount += partitions.size();
    }
    if (totalReplicaCount == 0 || _replica > _faultZoneWeight.size()) {
      return false;
    }

    // instance -> target (ideal) partition count
    Map<String, Float> targetPartitionCount = new HashMap<>();
    for (String liveInstance : _instanceFaultZone.keySet()) {
      long zoneWeight = _faultZoneWeight.get(_instanceFaultZone.get(liveInstance));
      float instanceRatioInZone = ((float) _instanceWeight.get(liveInstance)) / zoneWeight;
      // 1. if replica = fault zone, fault zone weight does not count, so calculate according to fault zone count.
      // 2. else, should consider fault zone weight to calculate expected threshold.
      float zonePartitions;
      if (_replica == _faultZoneWeight.size()) {
        zonePartitions = ((float) totalReplicaCount) / _faultZoneWeight.size();
      } else {
        zonePartitions = ((float) totalReplicaCount) * zoneWeight / _totalWeight;
      }
      targetPartitionCount.put(liveInstance, instanceRatioInZone * zonePartitions);
    }

    // Calculate the expected spikes
    // Assign spikes to each zone according to zone weight
    int totalOverflows = (int) totalReplicaCount % _instanceFaultZone.size();
    Map<String, Integer> maxZoneOverflows = new HashMap<>();
    for (String faultZoneName : _faultZoneWeight.keySet()) {
      float zoneWeight = _faultZoneWeight.get(faultZoneName);
      maxZoneOverflows.put(faultZoneName,
          (int) Math.ceil(((float) totalOverflows) * zoneWeight / _totalWeight));
    }

    Iterator<String> nodeIter = nodeToPartitionMap.keySet().iterator();
    while (nodeIter.hasNext()) {
      String instance = nodeIter.next();
      // Cleanup the existing mapping. Remove all non-active nodes from the mapping
      if (!_instanceFaultZone.containsKey(instance)) {
        List<String> partitions = nodeToPartitionMap.get(instance);
        addToReAssignPartition(toBeReassigned, partitions);
        partitions.clear();
        nodeIter.remove();
      }
    }

    List<String> orderedInstances = new ArrayList<>(_instanceFaultZone.keySet());
    // Different resource should shuffle nodes in different ways.
    Collections.shuffle(orderedInstances, new Random(randomSeed));
    for (String instance : orderedInstances) {
      if (!nodeToPartitionMap.containsKey(instance)) {
        continue;
      }
      // Cut off the exceed partitions compared with target partition count.
      List<String> partitions = nodeToPartitionMap.get(instance);
      int target = (int) (Math.floor(targetPartitionCount.get(instance)));
      if (partitions.size() > target) {
/*
        int maxZoneOverflow = maxZoneOverflows.get(_instanceFaultZone.get(instance));
        if (maxZoneOverflow > 0 && totalOverflows > 0) {
          // When fault zone has overflow capacity AND there are still remaining overflow partitions
          target = (int) (Math.ceil(targetPartitionCount.get(instance)));
          maxZoneOverflows.put(_instanceFaultZone.get(instance), maxZoneOverflow - 1);
          totalOverflows--;
        }
*/


        // Shuffle partitions to randomly pickup exceed ones. Ensure the algorithm generates consistent results when the inputs are the same.
        Collections.shuffle(partitions, new Random(instance.hashCode() * 31 + randomSeed));
        addToReAssignPartition(toBeReassigned, partitions.subList(target, partitions.size()));

        // Put the remaining partitions to the assignment, and record in fault zone partition list
        List<String> remainingPartitions = new ArrayList<>(partitions.subList(0, target));
        partitions.clear();
        nodeToPartitionMap.put(instance, remainingPartitions);
      }
      _faultZonePartitionMap.get(_instanceFaultZone.get(instance))
          .addAll(nodeToPartitionMap.get(instance));
    }

    // Reassign if any instances have space left.
    // Assign partition according to the target capacity, CAP at "Math.floor(target) + adjustment"
    int adjustment = 0;
    while (!toBeReassigned.isEmpty() && adjustment <= MAX_ADJUSTMENT) {
      partitionDealing(_instanceFaultZone.keySet(), toBeReassigned, _faultZonePartitionMap,
          _instanceFaultZone, nodeToPartitionMap, targetPartitionCount, randomSeed, adjustment++);
    }
    return toBeReassigned.isEmpty();
  }

  private void partitionDealing(Collection<String> instances,
      TreeMap<String, Integer> toBeReassigned, Map<String, Set<String>> faultZonePartitionMap,
      Map<String, String> faultZoneMap, final Map<String, List<String>> assignmentMap,
      final Map<String, Float> targetPartitionCount, final int randomSeed, final int targetAdjustment) {
    PriorityQueue<String> instanceQueue =
        new PriorityQueue<>(instances.size(), new Comparator<String>() {
          @Override
          public int compare(String node1, String node2) {
            int node1Load = assignmentMap.containsKey(node1) ? assignmentMap.get(node1).size() : 0;
            int node2Load = assignmentMap.containsKey(node2) ? assignmentMap.get(node2).size() : 0;
            if (node1Load == node2Load) {
              Float node1Target = targetPartitionCount.get(node1);
              Float node2Target = targetPartitionCount.get(node2);
              if (node1Target == node2Target) {
                return new Integer((node1 + randomSeed).hashCode()).compareTo((node2 + randomSeed).hashCode());
              } else {
                return node2Target.compareTo(node1Target);
              }
            } else {
              return node1Load - node2Load;
            }
          }
        });
    instanceQueue.addAll(instances);

    while (!toBeReassigned.isEmpty()) {
      boolean anyPartitionAssigned = false;
      Iterator<String> instanceIter = instanceQueue.iterator();
      while (instanceIter.hasNext()) {
        String instance = instanceIter.next();
        // Temporary remove the node from queue.
        // If any partition assigned to the instance, add it back to reset priority.
        instanceIter.remove();
        boolean partitionAssignedToInstance = false;
        String faultZoneStr = faultZoneMap.get(instance);
        List<String> partitions = assignmentMap.containsKey(instance) ?
            assignmentMap.get(instance) :
            new ArrayList<String>();
        int space =
            (int) (Math.floor(targetPartitionCount.get(instance))) + targetAdjustment - partitions
                .size();
        if (space > 0) {
          // Find a pending partition to locate
          for (String pendingPartition : toBeReassigned.navigableKeySet()) {
            if (!faultZonePartitionMap.get(faultZoneStr).contains(pendingPartition)) {
              if (!assignmentMap.containsKey(instance)) {
                assignmentMap.put(instance, partitions);
              }
              partitions.add(pendingPartition);
              faultZonePartitionMap.get(faultZoneStr).add(pendingPartition);
              if (toBeReassigned.get(pendingPartition) == 1) {
                toBeReassigned.remove(pendingPartition);
              } else {
                toBeReassigned.put(pendingPartition, toBeReassigned.get(pendingPartition) - 1);
              }
              // if any assignment is made:
              // this instance can hold more partitions in the future
              partitionAssignedToInstance = true;
              break;
            }
          }
        }
        if (partitionAssignedToInstance) {
          // Reset priority in the queue
          instanceQueue.add(instance);
          anyPartitionAssigned = true;
          break;
        }
      }
      if (!anyPartitionAssigned) {
        // if no pending partition is assigned to any instances in this loop, new assignment is not possible
        break;
      }
    }
  }

  private void addToReAssignPartition(TreeMap<String, Integer> toBeReassigned,
      List<String> partitions) {
    for (String partition : partitions) {
      if (!toBeReassigned.containsKey(partition)) {
        toBeReassigned.put(partition, 1);
      } else {
        toBeReassigned.put(partition, toBeReassigned.get(partition) + 1);
      }
    }
  }
}

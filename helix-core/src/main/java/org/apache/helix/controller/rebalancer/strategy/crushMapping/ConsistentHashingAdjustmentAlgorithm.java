package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.util.JenkinsHash;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ConsistentHashingAdjustmentAlgorithm {
  private static final int MAX_SELETOR_CACHE_SIZE = 1000;
  private static final int SELETOR_CACHE_EXPIRE = 3;

  private JenkinsHash _hashFunction;
  private ConsistentHashSelector _selector;
  Set<String> _activeInstances = new HashSet<>();

  // Instance -> FaultZone Tag
  private Map<String, String> _faultZoneMap = new HashMap<>();
  // Record existing partitions that are assigned to a fault zone
  private Map<String, Set<String>> _faultZonePartitionMap = new HashMap<>();

  // Cache records all known topology.
  private final static LoadingCache<Set<String>, ConsistentHashSelector> _selectorCache =
      CacheBuilder.newBuilder().maximumSize(MAX_SELETOR_CACHE_SIZE)
          .expireAfterAccess(SELETOR_CACHE_EXPIRE, TimeUnit.MINUTES)
          .build(new CacheLoader<Set<String>, ConsistentHashSelector>() {
            public ConsistentHashSelector load(Set<String> allInstances) {
              return new ConsistentHashSelector(allInstances);
            }
          });

  public ConsistentHashingAdjustmentAlgorithm(Topology topology, Collection<String> activeInstances)
      throws ExecutionException {
    _hashFunction = new JenkinsHash();
    Set<String> allInstances = new HashSet<>();
    // Get all instance related information.
    for (Node zone : topology.getFaultZones()) {
      for (Node instance : Topology.getAllLeafNodes(zone)) {
        allInstances.add(instance.getName());
        _faultZoneMap.put(instance.getName(), zone.getName());
        if (!_faultZonePartitionMap.containsKey(zone.getName())) {
          _faultZonePartitionMap.put(zone.getName(), new HashSet<String>());
        }
      }
    }
    _selector = _selectorCache.get(allInstances);
    _activeInstances.addAll(activeInstances);
  }

  public boolean computeMapping(Map<String, List<String>> nodeToPartitionMap, int randomSeed) {
    if (_activeInstances.isEmpty()) {
      return false;
    }

    Set<String> inactiveInstances = new HashSet<>();
    Map<String, Integer> toBeReassigned = new HashMap<>();
    // Remove all partition assignment to a non-live instance
    Iterator<String> nodeIter = nodeToPartitionMap.keySet().iterator();
    while (nodeIter.hasNext()) {
      String instance = nodeIter.next();
      List<String> partitions = nodeToPartitionMap.get(instance);
      if (!_activeInstances.contains(instance)) {
        inactiveInstances.add(instance);
        addToReAssignPartition(toBeReassigned, partitions);
        partitions.clear();
        nodeIter.remove();
      } else {
        _faultZonePartitionMap.get(_faultZoneMap.get(instance)).addAll(partitions);
      }
    }

    for (String partition : new ArrayList<>(toBeReassigned.keySet())) {
      int remainReplicas = toBeReassigned.get(partition);
      Set<String> conflictInstance = new HashSet<>();
      for (int index = 0; index < toBeReassigned.get(partition); index++) {
        Iterable<String> sortedInstances =
            _selector.getCircle(_hashFunction.hash(randomSeed, partition.hashCode(), index));
        Iterator<String> instanceItr = sortedInstances.iterator();
        while (instanceItr.hasNext()
            && conflictInstance.size() + inactiveInstances.size() != _selector.instanceSize) {
          String instance = instanceItr.next();
          if (!_activeInstances.contains(instance)) {
            inactiveInstances.add(instance);
          }
          if (inactiveInstances.contains(instance) || conflictInstance.contains(instance)) {
            continue;
          }
          Set<String> faultZonePartitions = _faultZonePartitionMap.get(_faultZoneMap.get(instance));
          if (faultZonePartitions.contains(partition)) {
            conflictInstance.add(instance);
            continue;
          }
          // insert this assignment
          if (!nodeToPartitionMap.containsKey(instance)) {
            nodeToPartitionMap.put(instance, new ArrayList<String>());
          }
          nodeToPartitionMap.get(instance).add(partition);
          faultZonePartitions.add(partition);
          remainReplicas--;
          break;
        }
      }
      if (remainReplicas == 0) {
        toBeReassigned.remove(partition);
      } else {
        toBeReassigned.put(partition, remainReplicas);
      }
    }

    return toBeReassigned.isEmpty();
  }

  private void addToReAssignPartition(Map<String, Integer> toBeReassigned,
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

class ConsistentHashSelector {
  private final static int DEFAULT_TOKENS_PER_INSTANCE = 1000;
  private final static JenkinsHash _hashFunction = new JenkinsHash();
  private final SortedMap<Long, String> circle = new TreeMap<>();
  protected int instanceSize = 0;

  public ConsistentHashSelector(Set<String> instances) {
    for (String instance : instances) {
      add(instance, DEFAULT_TOKENS_PER_INSTANCE);
      instanceSize++;
    }
  }

  private void add(String instance, long numberOfReplicas) {
    int instanceHashCode = instance.hashCode();
    for (int i = 0; i < numberOfReplicas; i++) {
      circle.put(_hashFunction.hash(instanceHashCode, i), instance);
    }
  }

  public Iterable<String> getCircle(long data) {
    if (circle.isEmpty()) {
      return null;
    }
    long hash = _hashFunction.hash(data);
    return circle.tailMap(hash).values();
  }
}

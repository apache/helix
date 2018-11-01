package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.helix.controller.rebalancer.topology.InstanceNode;
import org.apache.helix.controller.rebalancer.topology.Node;
import org.apache.helix.controller.rebalancer.topology.Topology;
import org.apache.helix.util.JenkinsHash;

public class ConsistentHashingAdjustmentAlgorithm {
  private static final int MAX_SELETOR_CACHE_SIZE = 1000;
  private static final int SELETOR_CACHE_EXPIRE = 3;

  private JenkinsHash _hashFunction;
  private ConsistentHashSelector _selector;
  Set<Node> _activeInstances = new HashSet<>();

  // Instance -> FaultZone Tag
  private Map<Node, Node> _faultZoneMap = new HashMap<>();
  // Record existing partitions that are assigned to a fault zone
  private Map<Node, Set<String>> _faultZonePartitionMap = new HashMap<>();

  // Cache records all known topology.
  private final static LoadingCache<Set<Node>, ConsistentHashSelector> _selectorCache =
      CacheBuilder.newBuilder().maximumSize(MAX_SELETOR_CACHE_SIZE)
          .expireAfterAccess(SELETOR_CACHE_EXPIRE, TimeUnit.MINUTES)
          .build(new CacheLoader<Set<Node>, ConsistentHashSelector>() {
            public ConsistentHashSelector load(Set<Node> allInstances) {
              return new ConsistentHashSelector(allInstances);
            }
          });

  public ConsistentHashingAdjustmentAlgorithm(Topology topology,
      Collection<String> activeInstanceNames) throws ExecutionException {
    _hashFunction = new JenkinsHash();
    Set<Node> allInstances = new HashSet<>();
    // Get all instance related information.
    for (Node zone : topology.getFaultZones()) {
      for (Node instance : Topology.getAllLeafNodes(zone)) {
        if (instance instanceof InstanceNode) {
          allInstances.add(instance);
          _faultZoneMap.put(instance, zone);
          if (!_faultZonePartitionMap.containsKey(zone)) {
            _faultZonePartitionMap.put(zone, new HashSet<String>());
          }
          if (activeInstanceNames.contains(((InstanceNode) instance).getInstanceName())) {
            _activeInstances.add(instance);
          }
        }
      }
    }
    _selector = _selectorCache.get(allInstances);
  }

  public boolean computeMapping(Map<Node, List<String>> nodeToPartitionMap, int randomSeed) {
    if (_activeInstances.isEmpty()) {
      return false;
    }

    Set<Node> inactiveInstances = new HashSet<>();
    Map<String, Integer> toBeReassigned = new HashMap<>();
    // Remove all partition assignment to a non-live instance
    Iterator<Node> nodeIter = nodeToPartitionMap.keySet().iterator();
    while (nodeIter.hasNext()) {
      Node instance = nodeIter.next();
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
      Set<Node> conflictInstance = new HashSet<>();
      for (int index = 0; index < toBeReassigned.get(partition); index++) {
        Iterable<Node> sortedInstances =
            _selector.getCircle(_hashFunction.hash(randomSeed, partition.hashCode(), index));
        Iterator<Node> instanceItr = sortedInstances.iterator();
        while (instanceItr.hasNext()
            && conflictInstance.size() + inactiveInstances.size() != _selector.instanceSize) {
          Node instance = instanceItr.next();
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
  private final SortedMap<Long, Node> circle = new TreeMap<>();
  protected int instanceSize = 0;

  public ConsistentHashSelector(Set<Node> instances) {
    for (Node instance : instances) {
      add(instance, DEFAULT_TOKENS_PER_INSTANCE);
      instanceSize++;
    }
  }

  private void add(Node instance, long numberOfReplicas) {
    int instanceHashCode = instance.hashCode();
    for (int i = 0; i < numberOfReplicas; i++) {
      circle.put(_hashFunction.hash(instanceHashCode, i), instance);
    }
  }

  public Iterable<Node> getCircle(long data) {
    if (circle.isEmpty()) {
      return null;
    }
    long hash = _hashFunction.hash(data);
    return circle.tailMap(hash).values();
  }
}

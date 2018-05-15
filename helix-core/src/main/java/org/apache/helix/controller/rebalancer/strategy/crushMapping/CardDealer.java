package org.apache.helix.controller.rebalancer.strategy.crushMapping;

import java.util.List;
import java.util.Map;

public interface CardDealer {
  boolean computeMapping(Map<String, List<String>> nodeToPartitionMap, int randomSeed);
}

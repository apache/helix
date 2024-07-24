package org.apache.helix.controller.rebalancer.condition;

public interface RebalanceCondition {
  boolean shouldPerformRebalance();
}

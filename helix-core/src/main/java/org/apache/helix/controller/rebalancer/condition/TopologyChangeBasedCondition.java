package org.apache.helix.controller.rebalancer.condition;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;

public class TopologyChangeBasedCondition implements RebalanceCondition {
  @Override
  public boolean shouldPerformRebalance(ResourceControllerDataProvider cache) {
    // TODO: implement the condition check for topology change
    return false;
  }
}

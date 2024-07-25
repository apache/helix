package org.apache.helix.controller.rebalancer.condition;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;

public class ConfigChangeBasedCondition implements RebalanceCondition {
  @Override
  public boolean shouldPerformRebalance(ResourceControllerDataProvider cache) {
    // TODO: implement the condition check for config change
    return false;
  }
}

package org.apache.helix.controller.rebalancer.condition;

import java.util.ArrayList;
import java.util.List;

public class RebalanceConditionsBuilder {
  private final List<RebalanceCondition> _rebalanceConditions = new ArrayList<>();

  public RebalanceConditionsBuilder withConfigChangeBasedCondition() {
    _rebalanceConditions.add(new ConfigChangeBasedCondition());
    return this;
  }

  public RebalanceConditionsBuilder withTopologyChangeBasedCondition() {
    _rebalanceConditions.add(new TopologyChangeBasedCondition());
    return this;
  }

  public List<RebalanceCondition> build() {
    return _rebalanceConditions;
  }
}

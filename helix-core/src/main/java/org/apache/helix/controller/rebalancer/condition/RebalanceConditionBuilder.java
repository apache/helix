package org.apache.helix.controller.rebalancer.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

public class RebalanceConditionBuilder {
  private final List<RebalanceCondition> _rebalanceConditions = new ArrayList<>();

  static class SimpleCondition implements RebalanceCondition {
    private final BooleanSupplier _condition;

    public SimpleCondition(BooleanSupplier condition) {
      this._condition = condition;
    }

    @Override
    public boolean shouldPerformRebalance() {
      return _condition.getAsBoolean();
    }
  }

  static class AndCondition implements RebalanceCondition {
    protected final List<RebalanceCondition> _conditions;

    public AndCondition(List<RebalanceCondition> conditions) {
      this._conditions = conditions;
    }

    @Override
    public boolean shouldPerformRebalance() {
      for (RebalanceCondition condition : _conditions) {
        if (!condition.shouldPerformRebalance()) {
          return false;
        }
      }
      return true;
    }
  }

  public RebalanceConditionBuilder addCondition(BooleanSupplier condition) {
    _rebalanceConditions.add(new SimpleCondition(condition));
    return this;
  }

  public RebalanceCondition build() {
    return new AndCondition(_rebalanceConditions);
  }
}

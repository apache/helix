package org.apache.helix.controller.rebalancer.condition;

import java.util.ArrayList;
import java.util.Arrays;
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
    public boolean evaluate() {
      return _condition.getAsBoolean();
    }
  }

  abstract static class CompositeCondition implements RebalanceCondition {
    protected final List<RebalanceCondition> _conditions;

    public CompositeCondition(List<RebalanceCondition> conditions) {
      this._conditions = conditions;
    }
  }

  static class AndCondition extends CompositeCondition {
    public AndCondition(List<RebalanceCondition> conditions) {
      super(conditions);
    }

    @Override
    public boolean evaluate() {
      for (RebalanceCondition condition : _conditions) {
        if (!condition.evaluate()) {
          return false;
        }
      }
      return true;
    }
  }

  static class OrCondition extends CompositeCondition {
    public OrCondition(List<RebalanceCondition> conditions) {
      super(conditions);
    }

    @Override
    public boolean evaluate() {
      for (RebalanceCondition condition : _conditions) {
        if (condition.evaluate()) {
          return true;
        }
      }
      return false;
    }
  }

  public RebalanceConditionBuilder addCondition(BooleanSupplier condition) {
    _rebalanceConditions.add(new SimpleCondition(condition));
    return this;
  }

  public RebalanceConditionBuilder addAndCondition(RebalanceCondition... nestedConditions) {
    _rebalanceConditions.add(new AndCondition(Arrays.asList(nestedConditions)));
    return this;
  }

  public RebalanceConditionBuilder addOrCondition(RebalanceCondition... nestedConditions) {
    _rebalanceConditions.add(new OrCondition(Arrays.asList(nestedConditions)));
    return this;
  }

  public RebalanceCondition buildAnd() {
    return new AndCondition(_rebalanceConditions);
  }

  public RebalanceCondition buildOr() {
    return new OrCondition(_rebalanceConditions);
  }
}

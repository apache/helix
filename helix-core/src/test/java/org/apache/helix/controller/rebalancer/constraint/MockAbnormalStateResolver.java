package org.apache.helix.controller.rebalancer.constraint;

import java.util.List;
import java.util.Map;

import org.apache.helix.api.rebalancer.constraint.AbnormalStateResolver;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;

/**
 * A mock abnormal state resolver for supporting tests.
 * It always return dummy result.
 */
public class MockAbnormalStateResolver implements AbnormalStateResolver {
  @Override
  public boolean isCurrentStatesValid(final CurrentStateOutput currentStateOutput,
      final String resourceName, final Partition partition,
      final StateModelDefinition stateModelDef) {
    // By default, all current states are valid.
    return true;
  }

  public Map<String, String> computeRecoveryAssignment(final CurrentStateOutput currentStateOutput,
      final String resourceName, final Partition partition,
      final StateModelDefinition stateModelDef, final List<String> preferenceList) {
    throw new UnsupportedOperationException("The mock resolver won't recover abnormal states.");
  }
}

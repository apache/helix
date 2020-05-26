package org.apache.helix.api.rebalancer.constraint;

import java.util.List;
import java.util.Map;

import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;

/**
 * A generic interface to find and recover if the partition has abnormal current states.
 */
public interface AbnormalStateResolver {
  /**
   * A placeholder which will be used when the resolver is not specified.
   * This is a dummy class that does not really functional.
   */
  AbnormalStateResolver DUMMY_STATE_RESOLVER = new AbnormalStateResolver() {
    public boolean isCurrentStatesValid(final CurrentStateOutput currentStateOutput,
        final String resourceName, final Partition partition,
        final StateModelDefinition stateModelDef) {
      // By default, all current states are valid.
      return true;
    }
    public Map<String, String> computeRecoveryAssignment(final CurrentStateOutput currentStateOutput,
        final String resourceName, final Partition partition,
        final StateModelDefinition stateModelDef, final List<String> preferenceList) {
      throw new UnsupportedOperationException("This resolver won't recover abnormal states.");
    }
  };

  /**
   * Check if the current states of the specified partition is valid.
   * @param currentStateOutput
   * @param resourceName
   * @param partition
   * @param stateModelDef
   * @return true if the current states of the specified partition is valid.
   */
  boolean isCurrentStatesValid(final CurrentStateOutput currentStateOutput,
      final String resourceName, final Partition partition,
      final StateModelDefinition stateModelDef);

  /**
   * Compute a transient partition state assignment to fix the abnormal.
   * @param currentStateOutput
   * @param resourceName
   * @param partition
   * @param stateModelDef
   * @param preferenceList
   * @return the transient partition state assignment which remove the abnormal states.
   */
  Map<String, String> computeRecoveryAssignment(final CurrentStateOutput currentStateOutput,
      final String resourceName, final Partition partition,
      final StateModelDefinition stateModelDef, final List<String> preferenceList);
}

package org.apache.helix.controller.rebalancer.waged.constraints;

import java.util.Map;

import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;


/**
 * Evaluate the proposed assignment according to the potential partition movements cost.
 * During partial rebalance, the cost is evaluated based on the existing best possible state
 * assignment; during global rebalance, the cost is evaluated based on the existing baseline
 * assignment
 */
public class PartitionMovementConstraint extends AbstractPartitionMovementConstraint {
  @Override
  protected Map<String, String> getStateMap(AssignableReplica replica,
      ClusterContext clusterContext) {
    return super.getStateMapFromAssignment(replica, clusterContext.getBestPossibleAssignment());
  }
}

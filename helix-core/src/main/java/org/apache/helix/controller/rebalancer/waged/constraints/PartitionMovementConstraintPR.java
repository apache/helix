package org.apache.helix.controller.rebalancer.waged.constraints;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;

/**
 * Evaluate the proposed assignment according to the potential partition movements cost.
 * The cost is evaluated based on the difference between the old assignment and the new assignment.
 * This constraint is for Partial Rebalance only, and the calculation is based on two previous
 * assignments:
 * - Baseline assignment that is calculated regardless of the node state (online/offline), which is
 * the result of Global Rebalance;
 * - Previous Best Possible assignment, which is the result of the previous Partial Rebalance
 * Any change to these two assignments will increase the partition movements cost, so that the
 * evaluated score will become lower.
 */
public class PartitionMovementConstraintPR extends PartitionMovementConstraint {
  @Override
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    if (clusterContext.getScopeType() != ClusterModelProvider.RebalanceScopeType.PARTIAL) {
      return MIN_SCORE;
    }
    return super.getAssignmentScore(node, replica, clusterContext);
  }
}

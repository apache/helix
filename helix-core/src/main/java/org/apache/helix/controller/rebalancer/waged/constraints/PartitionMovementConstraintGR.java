package org.apache.helix.controller.rebalancer.waged.constraints;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;

/**
 * Evaluate the proposed assignment according to the potential partition movements cost.
 * The cost is evaluated based on the difference between the old assignment and the new assignment.
 * This constraint is for Global Rebalance only, and the calculation is based on the previous
 * baseline assignment, which is calculated by the previous global rebalance.
 * Any change to these two assignments will increase the partition movements cost, so that the
 * evaluated score will become lower.
 */
public class PartitionMovementConstraintGR extends PartitionMovementConstraint {
  @Override
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    if (clusterContext.getScopeType() != ClusterModelProvider.RebalanceScopeType.GLOBAL_BASELINE) {
      return MIN_SCORE;
    }
    return super.getAssignmentScore(node, replica, clusterContext);
  }
}

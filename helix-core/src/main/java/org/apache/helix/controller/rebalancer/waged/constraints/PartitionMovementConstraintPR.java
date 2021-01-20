package org.apache.helix.controller.rebalancer.waged.constraints;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;


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

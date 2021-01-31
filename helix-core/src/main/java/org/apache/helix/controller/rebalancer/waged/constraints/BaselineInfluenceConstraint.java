package org.apache.helix.controller.rebalancer.waged.constraints;

import java.util.Map;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;


/**
 * Evaluate the proposed assignment according to the potential partition movements cost based on
 * the baseline assignment's influence. This constraint is only for baseline influence when best
 * possible assignment does not provide a preference, and therefore requires the best possible
 * assignment to exist and to give no score to this proposed assignment.
 */
public class BaselineInfluenceConstraint extends AbstractPartitionMovementConstraint {
  @Override
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    Map<String, String> bestPossibleAssignment =
        getStateMap(replica, clusterContext.getBestPossibleAssignment());
    Map<String, String> baselineAssignment =
        getStateMap(replica, clusterContext.getBaselineAssignment());
    String nodeName = node.getInstanceName();
    String state = replica.getReplicaState();
    // This constraint only works
    // 1. when best possible assignment is not empty, because PartitionMovementConstraint will
    // treat baseline as the sole reference if missing best possible;
    // 2. and when best possible score is 0, because this constraint is only for baseline
    // influence in absence of best possible assignment for this node-replica pair.
    if (bestPossibleAssignment.isEmpty()
        || calculateAssignmentScore(nodeName, state, bestPossibleAssignment) > 0) {
      return getMinScore();
    }
    return calculateAssignmentScore(nodeName, state, baselineAssignment);
  }
}

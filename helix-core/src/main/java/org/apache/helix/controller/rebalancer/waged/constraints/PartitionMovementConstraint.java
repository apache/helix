package org.apache.helix.controller.rebalancer.waged.constraints;

import java.util.Map;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;


/**
 * Evaluate the proposed assignment according to the potential partition movements cost.
 * Best possible assignment is the sole reference, and if it's missing, use baseline assignment
 * instead.
 */
public class PartitionMovementConstraint extends AbstractPartitionMovementConstraint {
  protected double getAssignmentScore(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext) {
    Map<String, String> bestPossibleAssignment =
        getStateMap(replica, clusterContext.getBestPossibleAssignment());
    Map<String, String> baselineAssignment =
        getStateMap(replica, clusterContext.getBaselineAssignment());
    String nodeName = node.getInstanceName();
    String state = replica.getReplicaState();

    if (bestPossibleAssignment.isEmpty()) {
      return calculateAssignmentScore(nodeName, state, baselineAssignment);
    }
    return calculateAssignmentScore(nodeName, state, bestPossibleAssignment);
  }
}

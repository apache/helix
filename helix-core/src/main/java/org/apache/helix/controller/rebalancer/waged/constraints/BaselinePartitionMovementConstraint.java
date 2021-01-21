package org.apache.helix.controller.rebalancer.waged.constraints;

import java.util.Map;

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;

/**
 * Evaluate the proposed assignment according to the potential partition movements cost.
 * The cost is evaluated based on the existing baseline assignment.
 */
public class BaselinePartitionMovementConstraint extends PartitionMovementConstraint {
  @Override
  protected Map<String, String> getReplicaStateMap(AssignableReplica replica,
      ClusterContext clusterContext) {
    return super.getStateMap(replica, clusterContext.getBaselineAssignment());
  }
}

package org.apache.helix.experiment;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModelBuilder;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;

import com.google.common.collect.ImmutableMap;

public class RebalanceAlgorithmAnalysis {
  public static void main(String[] args) throws HelixRebalanceException {
    MockClusterModel initClusterModel = new MockClusterModelBuilder("TestCluster").build();
    RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory
        .getInstance(ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 2,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 8));
    OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(initClusterModel);
  }
}

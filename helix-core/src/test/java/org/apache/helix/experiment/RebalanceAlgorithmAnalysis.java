package org.apache.helix.experiment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModelBuilder;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.model.ResourceAssignment;


public class RebalanceAlgorithmAnalysis {
  public static void main(String[] args) throws HelixRebalanceException {
    MockClusterModel clusterModel = new MockClusterModelBuilder("TestCluster")
        .setPartitionUsageSampleMethod(capacity -> capacity)
        .build();
    RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory
        .getInstance(ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 2,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 8));

    OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
    Map<String, ResourceAssignment> bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();

    System.out.println("Initial Assignment");
    System.out.println(clusterModel.getCoefficientOfVariationAsEvenness());
    System.out.println(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment));
    for (int i = 0; i < 100; i++) {
      clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
      List<AssignableNode> currentNodes = new ArrayList<>(clusterModel.getAssignableNodes());
      AssignableNode crashNode = currentNodes.get(new Random().nextInt(currentNodes.size()));
      clusterModel.onInstanceCrash(crashNode); // crash on one random instance
      optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
      System.out.println("After node crash.");
      System.out.println(clusterModel.getCoefficientOfVariationAsEvenness());
      System.out.println(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment));
      bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
      clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
      clusterModel.onInstanceAddition(crashNode); // add the instance back
      optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
      System.out.println("After adding back the crash node.");
      System.out.println(clusterModel.getCoefficientOfVariationAsEvenness());
      System.out.println(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment));
      bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
    }
  }
}

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
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
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

    AssignableNode crashNode = null;
    Map<String, ResourceAssignment> bestPossibleAssignment = Collections.emptyMap();
    for (int i = 0; i < 10; i++) {
      clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
      List<AssignableNode> nodes = new ArrayList<>(clusterModel.getAssignableNodesAsSet());
      List<AssignableReplica> unAssignedReplicas =
          crashNode == null ? clusterModel.getUnassignedReplicas() : clusterModel.onInstanceCrash(crashNode);

      if (crashNode != null) {
        nodes.remove(crashNode);
      }
      MockClusterModel testClusterModel = new MockClusterModel(clusterModel.getContext(), new HashSet<>(unAssignedReplicas), new HashSet<>(nodes));
      OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(testClusterModel);
      System.out.println("After node crash");
      System.out.println(clusterModel.getCoefficientOfVariationAsEvenness());
      System.out.println(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment));

      bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
      if (crashNode != null) {
        //add the node back
        nodes.add(crashNode);
        Set<AssignableReplica> replicas = new HashSet<>(crashNode.getAssignedReplicas());
        for (AssignableReplica replica : replicas) {
          crashNode.release(replica);
        }
        unAssignedReplicas =  testClusterModel.onInstanceAddition(crashNode);
        testClusterModel = new MockClusterModel(clusterModel.getContext(), new HashSet<>(unAssignedReplicas), new HashSet<>(nodes));
        optimalAssignment = rebalanceAlgorithm.calculate(testClusterModel);
        System.out.println("After add the node back");
        System.out.println(clusterModel.getCoefficientOfVariationAsEvenness());
        System.out.println(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment));
        bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
      }
      crashNode = nodes.get(new Random().nextInt(nodes.size()));
    }
  }
}

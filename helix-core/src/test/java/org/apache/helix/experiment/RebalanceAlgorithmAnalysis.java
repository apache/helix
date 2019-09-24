package org.apache.helix.experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.MockClusterModelBuilder;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RebalanceAlgorithmAnalysis {
  private static float[] getWeights() {
    float[] weights = new float[5];
    for (int i = 0; i < weights.length; i++) {
      weights[i] = new Random().nextFloat();
    }

    return weights;
  }

  public static void main(String[] args) throws HelixRebalanceException, IOException {
    List<String> columns = ImmutableList.of("evenness", "movement", "PartitionMovement",
        "InstancePartitionCount", "ResourcePartitionCount", "ResourceTopStateCount",
        "MaxCapacityKeyUsage", "worstEvenness", "worstMovement");
    FileWriter fileWriter = new FileWriter("dataset.csv");
    fileWriter.append(String.join(",", columns));
    fileWriter.append("\n");

    Random random = new Random();
    for (int r = 0; r < 10; r++) {
      int evennessPreference = random.nextInt(10) + 1;
      int movementPreference = random.nextInt(10) + 1;
      for (int i = 0; i < 100; i++) {
        MockClusterModel clusterModel = new MockClusterModelBuilder("TestCluster")
                .setZoneCount(5)
                .setInstanceCountPerZone(10)
                .setResourceCount(3)
                .setPartitionCountPerResource(10)
                .setMaxPartitionsPerInstance(10).build();

        float[] weights = getWeights();
        RebalanceAlgorithm rebalanceAlgorithm =
                ConstraintBasedAlgorithmFactory
                        .getInstance(
                                ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evennessPreference,
                                        ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, movementPreference), weights);
        float totalPartitionsCount = clusterModel.getContext().getAllReplicas().size();
        OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
        Map<String, ResourceAssignment> bestPossibleAssignment =
                optimalAssignment.getOptimalResourceAssignment();
        clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);

        double worstEvenness = clusterModel.getCoefficientOfVariationAsEvenness().get("size");
        float worstMovements = 0f;

        List<AssignableNode> currentNodes = new ArrayList<>(clusterModel.getAssignableNodes());
        for (AssignableNode crashNode : currentNodes) {
          clusterModel.onInstanceCrash(crashNode); // crash on the instance
          optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
          // after a node crashes
          worstEvenness =
                  Math.max(clusterModel.getCoefficientOfVariationAsEvenness().get("size"), worstEvenness);
          worstMovements = Math
                  .max(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment)
                          / totalPartitionsCount, worstMovements);
          clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
          clusterModel.onInstanceAddition(crashNode); // add the instance back
          optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
          // System.out.println("After adding back the crash node.");
          worstEvenness =
                  Math.max(clusterModel.getCoefficientOfVariationAsEvenness().get("size"), worstEvenness);
          worstMovements = Math
                  .max(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment)
                          / totalPartitionsCount, worstMovements);
          bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
        }

        List<String> rows = new ArrayList<>();
        rows.add(String.valueOf(evennessPreference));
        rows.add(String.valueOf(movementPreference));
        float evennessRatio = (float) evennessPreference / (evennessPreference + movementPreference);
        float movementRatio = (float) movementPreference / (evennessPreference + movementPreference);
        rows.add(String.valueOf(weights[0] * movementRatio));
        for (int j = 1; j < weights.length; j++) {
          rows.add(String.valueOf(weights[j] * evennessRatio));
        }
        rows.add(String.valueOf(worstEvenness));
        rows.add(String.valueOf(worstMovements));

        fileWriter.append(String.join(",", rows)).append("\n");
      }
    }

    fileWriter.flush();
    fileWriter.close();
  }
}

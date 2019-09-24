package org.apache.helix.experiment;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
  public static void main(String[] args) throws HelixRebalanceException, IOException {
    List<String> columns = ImmutableList.of("evenness", "movement", "PartitionMovement", "InstancePartitionCount", "ResourcePartitionCount",
        "ResourceTopStateCount", "MaxCapacityKeyUsage", "instanceCount", "resourceCount", "partitionCount", "worstEvenness", "worstMovement");
    FileWriter fileWriter = new FileWriter("dataset.csv");
    fileWriter.append(String.join(",", columns));
    fileWriter.append("\n");

    for (int r = 0; r < 1000; r++) {
      List<Integer> configs = new ArrayList<>();
      Random random = new Random();
      for (int i = 0; i < columns.size() - 2; i++) {
        int num = random.nextInt(100) + 1;
        configs.add(num);
        fileWriter.append(String.valueOf(i < 2 ? num : num / 100f) + ",");
      }

      int instanceCount = configs.get(7);
      int resourceCount = configs.get(8);
      int partitionsCount = configs.get(9);
      MockClusterModel clusterModel = new MockClusterModelBuilder("TestCluster").setZoneCount(5)
              .setInstanceCountPerZone(configs.get(instanceCount))
              .setResourceCount(configs.get(resourceCount))
              .setPartitionCountPerResource(partitionsCount)
              .setMaxPartitionsPerInstance(30)
              .build();

      Float[] weights = configs.subList(2, 7).stream().map(x -> x / 100f).toArray(Float[]::new);
      int evenness = configs.get(0);
      int movements = configs.get(1);

      RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory.getInstance(
              ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evenness,
                      ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, movements), weights);
      float totalPartitionsCount = clusterModel.getContext().getAllReplicas().size();

      System.out.println(totalPartitionsCount);
      OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);

      System.out.println("Initial Assignment");
      System.out.println(clusterModel.getCoefficientOfVariationAsEvenness());
      System.out.println(
              clusterModel.getTotalMovedPartitionsCount(optimalAssignment, Collections.emptyMap()) / totalPartitionsCount);

      double worstEvenness = clusterModel.getCoefficientOfVariationAsEvenness().get("size");
      fileWriter.append(String.valueOf(worstEvenness) + ",");
      fileWriter.append(String.valueOf(1f) + "\n");
      float worstMovements = 1f;
//      Map<String, ResourceAssignment> bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
//      for (int i = 0; i < 20; i++) {
//        clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
//        List<AssignableNode> currentNodes = new ArrayList<>(clusterModel.getAssignableNodes());
//        AssignableNode crashNode = currentNodes.get(new Random().nextInt(currentNodes.size()));
//        clusterModel.onInstanceCrash(crashNode); // crash on one random instance
//        optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
//        // after a node crashes
//        worstEvenness = Math.max(clusterModel.getCoefficientOfVariationAsEvenness().get("size"), worstEvenness);
//        worstMovements = Math.max(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment) / totalPartitionsCount, worstMovements);
//        clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
//        clusterModel.onInstanceAddition(crashNode); // add the instance back
//        optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
////      System.out.println("After adding back the crash node.");
//        worstEvenness = Math.max(clusterModel.getCoefficientOfVariationAsEvenness().get("size"), worstEvenness);
//        worstMovements = Math.max(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment) / totalPartitionsCount, worstMovements);
//        bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
//      }
//
//      fileWriter.append(String.valueOf(worstEvenness) + ",");
//      fileWriter.append(String.valueOf(worstMovements) + "\n");
//    }
    }

    fileWriter.flush();
    fileWriter.close();
  }
}

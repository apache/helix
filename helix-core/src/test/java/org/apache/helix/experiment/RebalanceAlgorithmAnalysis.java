package org.apache.helix.experiment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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


public class RebalanceAlgorithmAnalysis {
  public static void main(String[] args) throws HelixRebalanceException, IOException {
    FileWriter fileWriter = new FileWriter("dataset.csv");
    List<String> columns = ImmutableList.of("evenness", "movement", "PartitionMovement", "InstancePartitionCount", "ResourcePartitionCount",
        "ResourceTopStateCount", "MaxCapacityKeyUsage", "worstEvenness", "worstMovement");
    fileWriter.append(String.join(",", columns));
    fileWriter.append("\n");

    for (int r = 0; r < 500; r++) {
      List<Integer> configs = new ArrayList<>();
      Random random = new Random();
      for (int i = 0; i < columns.size(); i++) {
        int num = random.nextInt(10);
        configs.add(num);
        fileWriter.append(String.valueOf(i < 2 ? num: num/10f) + ",");
      }

      MockClusterModel clusterModel = new MockClusterModelBuilder("TestCluster").setZoneCount(5)
          .setInstanceCountPerZone(10)
          .setResourceCount(10)
          .setPartitionCountPerResource(10)
          .setMaxPartitionsPerInstance(30)
          .setPartitionUsageSampleMethod(capacity -> capacity)
          .build();

      Float[] weights = configs.subList(2, configs.size()).stream().map(x -> x/10f).toArray(Float[]::new);
      RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory.getInstance(
          ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, configs.get(0),
              ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, configs.get(1)), weights);
      float totalPartitionsCount = clusterModel.getContext().getAllReplicas().size();
      OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
      Map<String, ResourceAssignment> bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();

      System.out.println("Initial Assignment");
      System.out.println(clusterModel.getCoefficientOfVariationAsEvenness());
      System.out.println(
          clusterModel.getTotalMovedPartitionsCount(optimalAssignment, Collections.emptyMap()) / totalPartitionsCount);
      double worstEvenness = -1f;
      float worstMovements = -1f;
      for (int i = 0; i < 100; i++) {
        clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
        List<AssignableNode> currentNodes = new ArrayList<>(clusterModel.getAssignableNodes());
        AssignableNode crashNode = currentNodes.get(new Random().nextInt(currentNodes.size()));
        clusterModel.onInstanceCrash(crashNode); // crash on one random instance
        optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
        // after a node crashes
        worstEvenness = Math.max(clusterModel.getCoefficientOfVariationAsEvenness().get("size"), worstEvenness);
        worstMovements = Math.max(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment) / totalPartitionsCount, worstMovements);
        clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
        clusterModel.onInstanceAddition(crashNode); // add the instance back
        optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
//      System.out.println("After adding back the crash node.");
        worstEvenness = Math.max(clusterModel.getCoefficientOfVariationAsEvenness().get("size"), worstEvenness);
        worstMovements = Math.max(clusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment) / totalPartitionsCount, worstMovements);
        bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
      }

      fileWriter.append(String.valueOf(worstEvenness) + ",");
      fileWriter.append(String.valueOf(worstMovements) + "\n");
    }

    fileWriter.flush();
    fileWriter.close();
  }
}

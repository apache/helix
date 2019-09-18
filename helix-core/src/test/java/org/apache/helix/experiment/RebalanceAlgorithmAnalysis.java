package org.apache.helix.experiment;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelBuilder;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.model.ClusterConfig;

import static com.google.common.math.DoubleMath.*;


public class RebalanceAlgorithmAnalysis {
  public static void main(String[] args) throws HelixRebalanceException {
    ClusterModel clusterModel = new ClusterModelBuilder("TestCluster").build();
    RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory.getInstance(
        ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 2,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 8));
    OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
    List<AssignableNode> instances = new ArrayList<>(clusterModel.getAssignableNodes().values());
    // get the covariance as the evenness measurement
    Map<String, List<Integer>> usages = new HashMap<>();
    for (AssignableNode instance : instances) {
      Map<String, Integer> capacityUsage = instance.getCapacityUsage();
      for (String key : capacityUsage.keySet()) {
        usages.computeIfAbsent(key, k -> new ArrayList<>()).add(capacityUsage.get(key));
      }
    }
    System.out.println(usages);
    Map<String, Double> covariance = usages.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> RebalanceAlgorithmAnalysis.getCovariance(e.getValue())));
    System.out.println(covariance);
    // TODO: get the movement number as the movement measurement
  }

  private static double getCovariance(List<Integer> nums) {
    int sum = 0;
    double mean = mean(nums);
    for (int num : nums) {
      sum += Math.pow((num - mean), 2);
    }
    double std = Math.sqrt(sum / (nums.size() - 1));
    return std / mean;
  }
}

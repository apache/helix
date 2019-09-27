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
  private static List<Float> getConfigs(int size) {
    List<Float> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      result.add((float) round(new Random().nextDouble(), 2));
    }

    return result;
  }

  private static double round(double value, int scale) {
    return Math.round(value * Math.pow(10, scale)) / Math.pow(10, scale);
  }

  private static void writeToCSV(String fileName, List<String> columns, List<List<String>> rows)
      throws IOException {

    FileWriter csvWriter = new FileWriter(fileName);
    csvWriter.append(String.join(",", columns)).append("\n");
    for (List<String> row : rows) {
      csvWriter.append(String.join(",", row)).append("\n");
    }
    csvWriter.flush();
    csvWriter.close();
  }

  private static float[] getPrimitives(List<Float> values) {
    float[] r = new float[values.size()];
    for (int i = 0; i < values.size(); i++) {
      r[i] = values.get(i);
    }
    return r;
  }

  private static List<Float> updateWeight(int seed, List<Float> weights, int index) {
    int diff = (int) Math.pow(10, seed);
    List<Float> ret = new ArrayList<>(weights);
    ret.set(index, diff + ret.get(index));
    return ret;
  }

  private static List<String> getTrainingDataSet(float[] weights, MockClusterModel clusterModel)
      throws HelixRebalanceException {
    float totalPartitionsCount = clusterModel.getContext().getAllReplicas().size();
    Map<String, ResourceAssignment> initPossibleAssignment =
        clusterModel.getContext().getBestPossibleAssignment();

    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences = Collections.emptyMap();

    RebalanceAlgorithm rebalanceAlgorithm =
        ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);

    // create a list of new nodes
    List<AssignableNode> newNodes = MockClusterModelBuilder.createInstances("NewInstance", 10,
        "NewZone", ImmutableMap.of("size", 500), 30);

    // add these new nodes to the cluster
    clusterModel.onClusterExpansion(newNodes);
    OptimalAssignment clusterExpansionOptimalAssignment =
        rebalanceAlgorithm.calculate(clusterModel);
    double clusterExpansionEvenness =
        clusterModel.getCoefficientOfVariationAsEvenness().get("size");
    //TODO: check if there're movements between existing nodes
    double clusterExpansionMovements =
        clusterModel.getTotalMovedPartitionsCount(clusterExpansionOptimalAssignment,
            initPossibleAssignment) / totalPartitionsCount;
    // remove the newly added nodes
    clusterModel.onInstanceCrash(newNodes);
    OptimalAssignment instanceCrashOptimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
    double instanceCrashEvenness = clusterModel.getCoefficientOfVariationAsEvenness().get("size");
    double instanceCrashMovements =
        clusterModel.getTotalMovedPartitionsCount(clusterExpansionOptimalAssignment,
            initPossibleAssignment) / totalPartitionsCount;

    List<String> rows = new ArrayList<>();
    for (float weight : weights) {
      rows.add(String.valueOf(weight));
    }
    rows.add(String.valueOf(clusterExpansionEvenness));
    rows.add(String.valueOf(clusterExpansionMovements));

    return rows;
  }

  private static List<List<String>> onWeightChange(List<Float> initWeights,
      MockClusterModel clusterModel, int weightIndex) throws HelixRebalanceException {
    List<List<String>> result = new ArrayList<>();
    for (int seed = -1; seed < 5; seed++) {
      List<Float> settings = updateWeight(seed, initWeights, weightIndex);
      result.add(getTrainingDataSet(getPrimitives(settings), clusterModel));
    }

    return result;
  }

  public static void main(String[] args) throws HelixRebalanceException, IOException {
    MockClusterModel clusterModel = new MockClusterModelBuilder("TestCluster").setZoneCount(3)
        .setInstanceCountPerZone(10).setResourceCount(1).setPartitionCountPerResource(15)
        .setMaxPartitionsPerInstance(10).build();

    List<List<String>> result = new ArrayList<>();

    for (int r = 0; r < 10; r++) {
      clusterModel = new MockClusterModel(clusterModel);
      List<Float> settings = getConfigs(5);
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences = Collections.emptyMap();
      float[] weights = getPrimitives(settings);
      RebalanceAlgorithm rebalanceAlgorithm =
          ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);

      OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(clusterModel);
      Map<String, ResourceAssignment> bestPossibleAssignment =
          optimalAssignment.getOptimalResourceAssignment();
      clusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);

      for (int i = 0; i < settings.size(); i++) {
        result.addAll(onWeightChange(new ArrayList<>(settings), clusterModel, i));
      }
    }

    List<String> names =
        ImmutableList.of("PartitionMovement", "InstancePartitionCount", "ResourcePartitionCount",
            "ResourceTopStateCount", "MaxCapacityKeyUsage", "worstEvenness", "worstMovement");
    writeToCSV("dataset.csv", names, result);
  }
}

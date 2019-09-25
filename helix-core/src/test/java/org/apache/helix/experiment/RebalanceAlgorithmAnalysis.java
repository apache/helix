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
  private static List<Float> getConfigs(int size) {
    Random random = new Random();
    List<Float> result = new ArrayList<>();
    result.add((float) random.nextInt(10) + 1);
    result.add((float) random.nextInt(10) + 1);
    for (int i = 2; i < size; i++) {
      result.add((float) round(new Random().nextDouble(), 2));
    }

    return result;
  }

  public static double round(double value, int scale) {
    return Math.round(value * Math.pow(10, scale)) / Math.pow(10, scale);
  }

  public static void writeToCSV(String fileName, List<String> columns, List<List<String>> rows)
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

  public static List<List<String>> onEvennessPreferenceChange(List<Float> settings,
      MockClusterModel mockClusterModel) throws HelixRebalanceException {
    List<List<String>> result = new ArrayList<>();
    float totalPartitionsCount = mockClusterModel.getContext().getAllReplicas().size();
    Map<String, ResourceAssignment> bestPossibleAssignment =
        mockClusterModel.getContext().getBestPossibleAssignment();

    for (int evennessSetting = 0; evennessSetting < 10; evennessSetting++) {
      int movementPreference = Math.round(settings.get(1));
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences =
          ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evennessSetting,
              ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, movementPreference);

      float[] weights = getPrimitives(settings.subList(2, settings.size()));
      RebalanceAlgorithm rebalanceAlgorithm =
          ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);

      double worstEvenness = mockClusterModel.getMaxDifferenceAsEvenness().get("size");
      float worstMovements = 0f;

      List<AssignableNode> currentNodes = new ArrayList<>(mockClusterModel.getAssignableNodes());
      for (AssignableNode crashNode : currentNodes) {
        mockClusterModel.onInstanceCrash(crashNode); // crash on the instance
        OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(mockClusterModel);
        // after a node crashes.
        worstEvenness =
            Math.max(mockClusterModel.getMaxDifferenceAsEvenness().get("size"), worstEvenness);
        worstMovements = Math.max(
            mockClusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment)
                / totalPartitionsCount,
            worstMovements);
        mockClusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
        mockClusterModel.onInstanceAddition(crashNode); // add the instance back
        optimalAssignment = rebalanceAlgorithm.calculate(mockClusterModel);
        // "After adding back the crash node
        worstEvenness =
            Math.max(mockClusterModel.getMaxDifferenceAsEvenness().get("size"), worstEvenness);
        worstMovements = Math.max(
            mockClusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment)
                / totalPartitionsCount,
            worstMovements);
        bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
      }

      List<String> rows = new ArrayList<>();
      rows.add(String.valueOf(evennessSetting));
      rows.add(String.valueOf(movementPreference));
      float evennessRatio = (float) evennessSetting / (evennessSetting + movementPreference);
      float movementRatio = (float) movementPreference / (evennessSetting + movementPreference);
      rows.add(String.valueOf(weights[0] * movementRatio));
      for (int j = 1; j < weights.length; j++) {
        rows.add(String.valueOf(weights[j] * evennessRatio));
      }
      rows.add(String.valueOf(worstEvenness));
      rows.add(String.valueOf(worstMovements));

      result.add(rows);
    }

    return result;
  }

  public static List<List<String>> onMovementPreferenceChange(List<Float> settings,
      MockClusterModel mockClusterModel) throws HelixRebalanceException {
    List<List<String>> result = new ArrayList<>();
    float totalPartitionsCount = mockClusterModel.getContext().getAllReplicas().size();
    Map<String, ResourceAssignment> bestPossibleAssignment =
        mockClusterModel.getContext().getBestPossibleAssignment();

    for (int movementsSetting = 0; movementsSetting < 10; movementsSetting++) {
      int evennessPreference = Math.round(settings.get(0));
      int movementPreference = movementsSetting;
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences =
          ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evennessPreference,
              ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, movementPreference);

      float[] weights = getPrimitives(settings.subList(2, settings.size()));
      RebalanceAlgorithm rebalanceAlgorithm =
          ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);

      double worstEvenness = mockClusterModel.getMaxDifferenceAsEvenness().get("size");
      float worstMovements = 0f;

      List<AssignableNode> currentNodes = new ArrayList<>(mockClusterModel.getAssignableNodes());
      for (AssignableNode crashNode : currentNodes) {
        mockClusterModel.onInstanceCrash(crashNode); // crash on the instance
        OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(mockClusterModel);
        // after a node crashes.
        worstEvenness =
            Math.max(mockClusterModel.getMaxDifferenceAsEvenness().get("size"), worstEvenness);
        worstMovements = Math.max(
            mockClusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment)
                / totalPartitionsCount,
            worstMovements);
        mockClusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
        mockClusterModel.onInstanceAddition(crashNode); // add the instance back
        optimalAssignment = rebalanceAlgorithm.calculate(mockClusterModel);
        // "After adding back the crash node
        worstEvenness =
            Math.max(mockClusterModel.getMaxDifferenceAsEvenness().get("size"), worstEvenness);
        worstMovements = Math.max(
            mockClusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment)
                / totalPartitionsCount,
            worstMovements);
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

      result.add(rows);
    }

    return result;
  }

  public static List<List<String>> onWeightChange(List<Float> settings,
      MockClusterModel mockClusterModel, int weightIndex) throws HelixRebalanceException {
    List<List<String>> result = new ArrayList<>();
    float totalPartitionsCount = mockClusterModel.getContext().getAllReplicas().size();
    Map<String, ResourceAssignment> bestPossibleAssignment =
        mockClusterModel.getContext().getBestPossibleAssignment();

    for (int i = 0; i < 10; i++) {
      int evennessPreference = Math.round(settings.get(0));
      int movementPreference = Math.round(settings.get(1));
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences =
          ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evennessPreference,
              ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, movementPreference);

      // increase the weight of the soft constraint each time by 0.1f
      settings.set(weightIndex, settings.get(weightIndex) + 0.1f);
      float[] weights = getPrimitives(settings.subList(2, settings.size()));
      RebalanceAlgorithm rebalanceAlgorithm =
          ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);

      double worstEvenness = mockClusterModel.getMaxDifferenceAsEvenness().get("size");
      float worstMovements = 0f;

      List<AssignableNode> currentNodes = new ArrayList<>(mockClusterModel.getAssignableNodes());
      for (AssignableNode crashNode : currentNodes) {
        mockClusterModel.onInstanceCrash(crashNode); // crash on the instance
        OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(mockClusterModel);
        // after a node crashes.
        worstEvenness =
            Math.max(mockClusterModel.getMaxDifferenceAsEvenness().get("size"), worstEvenness);
        worstMovements = Math.max(
            mockClusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment)
                / totalPartitionsCount,
            worstMovements);
        mockClusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);
        mockClusterModel.onInstanceAddition(crashNode); // add the instance back
        optimalAssignment = rebalanceAlgorithm.calculate(mockClusterModel);
        // "After adding back the crash node
        worstEvenness =
            Math.max(mockClusterModel.getMaxDifferenceAsEvenness().get("size"), worstEvenness);
        worstMovements = Math.max(
            mockClusterModel.getTotalMovedPartitionsCount(optimalAssignment, bestPossibleAssignment)
                / totalPartitionsCount,
            worstMovements);
        bestPossibleAssignment = optimalAssignment.getOptimalResourceAssignment();
      }

      List<String> rows = new ArrayList<>();
      rows.add(String.valueOf(evennessPreference));
      rows.add(String.valueOf(i));
      float evennessRatio = (float) evennessPreference / (i + i);
      float movementRatio = (float) i / (i + i);
      rows.add(String.valueOf(weights[0] * movementRatio));
      for (int j = 1; j < weights.length; j++) {
        rows.add(String.valueOf(weights[j] * evennessRatio));
      }
      rows.add(String.valueOf(worstEvenness));
      rows.add(String.valueOf(worstMovements));

      result.add(rows);
    }

    return result;
  }

  public static void main(String[] args) throws HelixRebalanceException, IOException {
    MockClusterModel initClusterModel = new MockClusterModelBuilder("TestCluster").setZoneCount(3)
        .setInstanceCountPerZone(10).setResourceCount(1).setPartitionCountPerResource(15)
        .setMaxPartitionsPerInstance(10).build();
    List<List<String>> result = new ArrayList<>();

    List<Float> settings = getConfigs(7);
    int evennessPreference = Math.round(settings.get(0));
    int movementPreference = Math.round(settings.get(1));
    Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences =
        ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, evennessPreference,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, movementPreference);

    float[] weights = getPrimitives(settings.subList(2, settings.size()));
    RebalanceAlgorithm rebalanceAlgorithm =
        ConstraintBasedAlgorithmFactory.getInstance(preferences, weights);

    OptimalAssignment optimalAssignment = rebalanceAlgorithm.calculate(initClusterModel);
    Map<String, ResourceAssignment> bestPossibleAssignment =
        optimalAssignment.getOptimalResourceAssignment();
    initClusterModel.getContext().setBestPossibleAssignment(bestPossibleAssignment);

    result.addAll(onEvennessPreferenceChange(new ArrayList<>(settings), initClusterModel));
    result.addAll(onMovementPreferenceChange(new ArrayList<>(settings), initClusterModel));
    for (int i = 2; i < 7; i++) {
      result.addAll(onWeightChange(new ArrayList<>(settings), initClusterModel, i));
    }

    List<String> names = ImmutableList.of("evenness", "movement", "PartitionMovement",
        "InstancePartitionCount", "ResourcePartitionCount", "ResourceTopStateCount",
        "MaxCapacityKeyUsage", "worstEvenness", "worstMovement");
    writeToCSV("dataset.csv", names, result);
  }
}

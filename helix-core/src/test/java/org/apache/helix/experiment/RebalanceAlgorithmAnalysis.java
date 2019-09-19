package org.apache.helix.experiment;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.zookeeper.Op;

import static com.google.common.math.DoubleMath.*;

public class RebalanceAlgorithmAnalysis {
  public static void main(String[] args) throws HelixRebalanceException {
    ClusterModel clusterModel = new ClusterModelBuilder("TestCluster").build();
    RebalanceAlgorithm rebalanceAlgorithm = ConstraintBasedAlgorithmFactory
        .getInstance(ImmutableMap.of(ClusterConfig.GlobalRebalancePreferenceKey.EVENNESS, 2,
            ClusterConfig.GlobalRebalancePreferenceKey.LESS_MOVEMENT, 8));
    OptimalAssignment optimalAssignment1 = rebalanceAlgorithm.calculate(clusterModel);
    System.out.println(optimalAssignment1.getCoefficientOfVariationAsEvenness());
    System.out.println(optimalAssignment1.getTotalPartitionMovements(Collections.emptyMap()));
  }
}

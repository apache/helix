package org.apache.helix.controller.rebalancer.waged;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.apache.helix.util.RebalanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Global Rebalance does the baseline recalculation when certain changes happen.
 * The Global Baseline calculation does not consider any temporary status, such as participants' offline/disabled.
 * Baseline is used as an anchor for {@link PartialRebalanceRunner}. Its computation takes previous baseline as an input.
 * The Baseline is NOT directly propagated to the final output. It is consumed by the {link PartialRebalanceRunner}
 * as an important parameter.
 */
class GlobalRebalanceRunner implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(GlobalRebalanceRunner.class);

  // When any of the following change happens, the rebalancer needs to do a global rebalance which
  // contains 1. baseline recalculate, 2. partial rebalance that is based on the new baseline.
  private static final Set<HelixConstants.ChangeType> GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES =
      ImmutableSet
          .of(HelixConstants.ChangeType.RESOURCE_CONFIG, HelixConstants.ChangeType.IDEAL_STATE,
              HelixConstants.ChangeType.CLUSTER_CONFIG, HelixConstants.ChangeType.INSTANCE_CONFIG);

  // To calculate the baseline asynchronously
  private final ExecutorService _baselineCalculateExecutor;
  private final ResourceChangeDetector _changeDetector;
  private final AssignmentManager _assignmentManager;
  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final LatencyMetric _writeLatency;
  private final CountMetric _baselineCalcCounter;
  private final LatencyMetric _baselineCalcLatency;
  private final CountMetric _rebalanceFailureCount;

  private boolean _asyncGlobalRebalanceEnabled;

  public GlobalRebalanceRunner(AssignmentManager assignmentManager,
      AssignmentMetadataStore assignmentMetadataStore,
      MetricCollector metricCollector,
      LatencyMetric writeLatency,
      CountMetric rebalanceFailureCount,
      boolean isAsyncGlobalRebalanceEnabled) {
    _baselineCalculateExecutor = Executors.newSingleThreadExecutor();
    _assignmentManager = assignmentManager;
    _assignmentMetadataStore = assignmentMetadataStore;
    _changeDetector = new ResourceChangeDetector(true);
    _writeLatency = writeLatency;
    _baselineCalcCounter = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcCounter.name(),
        CountMetric.class);
    _baselineCalcLatency = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.GlobalBaselineCalcLatencyGauge.name(),
        LatencyMetric.class);
    _rebalanceFailureCount = rebalanceFailureCount;
    _asyncGlobalRebalanceEnabled = isAsyncGlobalRebalanceEnabled;
  }

  /**
   * Global rebalance calculates for a new baseline assignment.
   * The new baseline assignment will be persisted and leveraged by the partial rebalance.
   * @param clusterData
   * @param resourceMap
   * @param currentStateOutput
   * @param algorithm
   * @throws HelixRebalanceException
   */
  public void globalRebalance(ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      final CurrentStateOutput currentStateOutput, RebalanceAlgorithm algorithm) throws HelixRebalanceException {
    _changeDetector.updateSnapshots(clusterData);
    // Get all the changed items' information. Filter for the items that have content changed.
    final Map<HelixConstants.ChangeType, Set<String>> clusterChanges = _changeDetector.getAllChanges();
    Set<String> allAssignableInstances = clusterData.getAssignableInstances();

    if (clusterChanges.keySet().stream().anyMatch(GLOBAL_REBALANCE_REQUIRED_CHANGE_TYPES::contains)) {
      final boolean waitForGlobalRebalance = !_asyncGlobalRebalanceEnabled;
      // Calculate the Baseline assignment for global rebalance.
      Future<Boolean> result = _baselineCalculateExecutor.submit(() -> {
        try {
          // If the synchronous thread does not wait for the baseline to be calculated, the synchronous thread should
          // be triggered again after baseline is finished.
          // Set shouldTriggerMainPipeline to be !waitForGlobalRebalance
          doGlobalRebalance(clusterData, resourceMap, allAssignableInstances, algorithm,
              currentStateOutput, !waitForGlobalRebalance, clusterChanges);
        } catch (HelixRebalanceException e) {
          if (_asyncGlobalRebalanceEnabled) {
            _rebalanceFailureCount.increment(1L);
          }
          LOG.error("Failed to calculate baseline assignment!", e);
          return false;
        }
        return true;
      });
      if (waitForGlobalRebalance) {
        try {
          if (!result.get()) {
            throw new HelixRebalanceException("Failed to calculate for the new Baseline.",
                HelixRebalanceException.Type.FAILED_TO_CALCULATE);
          }
        } catch (InterruptedException | ExecutionException e) {
          throw new HelixRebalanceException("Failed to execute new Baseline calculation.",
              HelixRebalanceException.Type.FAILED_TO_CALCULATE, e);
        }
      }
    }
  }

  /**
   * Calculate and update the Baseline assignment
   * @param shouldTriggerMainPipeline True if the call should trigger a following main pipeline rebalance
   *                                   so the new Baseline could be applied to cluster.
   */
  private void doGlobalRebalance(ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap, Set<String> allAssignableInstances,
      RebalanceAlgorithm algorithm, CurrentStateOutput currentStateOutput, boolean shouldTriggerMainPipeline,
      Map<HelixConstants.ChangeType, Set<String>> clusterChanges)
      throws HelixRebalanceException {
    LOG.info("Start calculating the new baseline.");
    _baselineCalcCounter.increment(1L);
    _baselineCalcLatency.startMeasuringLatency();

    // Build the cluster model for rebalance calculation.
    // Note, for a Baseline calculation,
    // 1. Ignore node status (disable/offline).
    // 2. Use the previous Baseline as the only parameter about the previous assignment.
    Map<String, ResourceAssignment> currentBaseline =
        // TODO: Look into use of currentStateOutput here.
        _assignmentManager.getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());
    ClusterModel clusterModel;
    try {
      clusterModel = ClusterModelProvider.generateClusterModelForBaseline(clusterData, resourceMap,
          allAssignableInstances, clusterChanges, currentBaseline);
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to generate cluster model for global rebalance.",
          HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
    }

    Map<String, ResourceAssignment> newBaseline = WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm);
    boolean isBaselineChanged =
        _assignmentMetadataStore != null && _assignmentMetadataStore.isBaselineChanged(newBaseline);
    // Write the new baseline to metadata store
    if (isBaselineChanged) {
      System.out.println("Baseline has changed: Persisting the new baseline assignment.");

      System.out.println("All Assignable Instances: " + allAssignableInstances);

      Map<String, ResourceAssignment> oldBaseline = _assignmentMetadataStore.getBaseline();

//      System.out.println("Old Baseline: " + _assignmentMetadataStore.getBaseline());
//      System.out.println("New Baseline: " + newBaseline);

      for (String resource : newBaseline.keySet()) {
        ResourceAssignment oldAssignment = oldBaseline.get(resource);
        if (oldAssignment == null) {
          System.out.println("Resource " + resource + " is new.");
          continue;
        }
        ResourceAssignment assignment = newBaseline.get(resource);
        for (Partition partition : assignment.getMappedPartitions()) {
          Map<String, String> oldPartitionMap = oldAssignment.getReplicaMap(partition);
          Map<String, String> newPartitionMap = assignment.getReplicaMap(partition);
          printMapDifferences(oldPartitionMap, newPartitionMap);
        }
      }

      try {
        _writeLatency.startMeasuringLatency();
        _assignmentMetadataStore.persistBaseline(newBaseline);
        _writeLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException("Failed to persist the new baseline assignment.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    } else {
      System.out.println("No Baseline change detected.");

      System.out.println("All Assignable Instances: " + allAssignableInstances);
      LOG.debug("Assignment Metadata Store is null. Skip persisting the baseline assignment.");
    }
    _baselineCalcLatency.endMeasuringLatency();
    LOG.info("Global baseline calculation completed and has been persisted into metadata store.");

    if (isBaselineChanged && shouldTriggerMainPipeline) {
      LOG.info("Schedule a new rebalance after the new baseline calculation has finished.");
      RebalanceUtil.scheduleOnDemandPipeline(clusterData.getClusterName(), 0L, false);
    }
  }

  private static void printMapDifferences(Map<String, String> map1, Map<String, String> map2) {
    // StringBuilder to concatenate differences
    StringBuilder diffBuilder = new StringBuilder();

    // Iterate over the keys in map1
    for (String key : map1.keySet()) {
      String value1 = map1.get(key);
      String value2 = map2.get(key);

      // Compare values and append differences to the StringBuilder
      if (!value1.equals(value2)) {
        diffBuilder.append(key).append(": old: ").append(value1).append(", new: ").append(value2)
            .append(" | ");
      }
    }

    // Check for keys present in map2 but not in map1
    for (String key : map2.keySet()) {
      if (!map1.containsKey(key)) {
        diffBuilder.append(key).append(": old: null").append(", new: ").append(map2.get(key))
            .append(" | ");
      }
    }

    // Print the differences on one line
    String diffResult = diffBuilder.toString().trim();
    if (!diffResult.isEmpty()) {
      System.out.println("Differences: " + diffResult);
    }
  }

  public void setGlobalRebalanceAsyncMode(boolean isAsyncGlobalRebalanceEnabled) {
    _asyncGlobalRebalanceEnabled = isAsyncGlobalRebalanceEnabled;
  }

  public ResourceChangeDetector getChangeDetector() {
    return _changeDetector;
  }

  public void resetChangeDetector() {
    _changeDetector.resetSnapshots();
  }

  public void close() {
    if (_baselineCalculateExecutor != null) {
      _baselineCalculateExecutor.shutdownNow();
    }
  }
}

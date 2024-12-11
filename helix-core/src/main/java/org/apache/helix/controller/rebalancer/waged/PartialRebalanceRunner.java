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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.WagedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModelProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.metrics.MetricCollector;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.implementation.BaselineDivergenceGauge;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.apache.helix.util.ExecutorTaskUtil;
import org.apache.helix.util.RebalanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Compute the best possible assignment based on the Baseline and the previous Best Possible assignment.
 * The coordinator compares the previous Best Possible assignment with the current cluster state so as to derive a
 * minimal rebalance scope. In short, the rebalance scope only contains the following two types of partitions.
 * 1. The partition's current assignment becomes invalid.
 * 2. The Baseline contains some new partition assignments that do not exist in the current assignment.
 */
class PartialRebalanceRunner implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PartialRebalanceRunner.class);

  private final ExecutorService _bestPossibleCalculateExecutor;
  private final AssignmentManager _assignmentManager;
  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final BaselineDivergenceGauge _baselineDivergenceGauge;
  private final CountMetric _rebalanceFailureCount;
  private final CountMetric _partialRebalanceCounter;
  private final LatencyMetric _partialRebalanceLatency;

  private boolean _asyncPartialRebalanceEnabled;
  private Future<Boolean> _asyncPartialRebalanceResult;

  public PartialRebalanceRunner(AssignmentManager assignmentManager,
      AssignmentMetadataStore assignmentMetadataStore,
      MetricCollector metricCollector,
      CountMetric rebalanceFailureCount,
      boolean isAsyncPartialRebalanceEnabled) {
    _assignmentManager = assignmentManager;
    _assignmentMetadataStore = assignmentMetadataStore;
    _bestPossibleCalculateExecutor = Executors.newSingleThreadExecutor();
    _rebalanceFailureCount = rebalanceFailureCount;
    _asyncPartialRebalanceEnabled = isAsyncPartialRebalanceEnabled;

    _partialRebalanceCounter = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceCounter.name(),
        CountMetric.class);
    _partialRebalanceLatency = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.PartialRebalanceLatencyGauge
            .name(),
        LatencyMetric.class);
    _baselineDivergenceGauge = metricCollector.getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.BaselineDivergenceGauge.name(),
        BaselineDivergenceGauge.class);
  }

  public void partialRebalance(ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, final CurrentStateOutput currentStateOutput, RebalanceAlgorithm algorithm)
      throws HelixRebalanceException {
    // If partial rebalance is async and the previous result is not completed yet,
    // do not start another partial rebalance.
    if (_asyncPartialRebalanceEnabled && _asyncPartialRebalanceResult != null
        && !_asyncPartialRebalanceResult.isDone()) {
      return;
    }

    _asyncPartialRebalanceResult = _bestPossibleCalculateExecutor.submit(ExecutorTaskUtil.wrap(() -> {
      try {
        doPartialRebalance(clusterData, resourceMap, activeNodes, algorithm,
            currentStateOutput);
      } catch (HelixRebalanceException e) {
        if (_asyncPartialRebalanceEnabled) {
          _rebalanceFailureCount.increment(1L);
        }
        LOG.error("Failed to calculate best possible assignment!", e);
        return false;
      }
      return true;
    }));
    if (!_asyncPartialRebalanceEnabled) {
      try {
        if (!_asyncPartialRebalanceResult.get()) {
          throw new HelixRebalanceException("Failed to calculate for the new best possible.",
              HelixRebalanceException.Type.FAILED_TO_CALCULATE);
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new HelixRebalanceException("Failed to execute new best possible calculation.",
            HelixRebalanceException.Type.FAILED_TO_CALCULATE, e);
      }
    }
  }

  /**
   * Calculate and update the Best Possible assignment
   * If the result differ from the persisted result, persist it to memory (only if the version is not stale);
   * If persisted, trigger the pipeline so that main thread logic can run again.
   */
  private void doPartialRebalance(ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, RebalanceAlgorithm algorithm, CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
    LOG.info("Start calculating the new best possible assignment.");
    _partialRebalanceCounter.increment(1L);
    _partialRebalanceLatency.startMeasuringLatency();

    int newBestPossibleAssignmentVersion = -1;
    if (_assignmentMetadataStore != null) {
      newBestPossibleAssignmentVersion = _assignmentMetadataStore.getBestPossibleVersion() + 1;
    } else {
      LOG.debug("Assignment Metadata Store is null. Skip getting best possible assignment version.");
    }

    // Read the baseline from metadata store
    Map<String, ResourceAssignment> currentBaseline =
        _assignmentManager.getBaselineAssignment(_assignmentMetadataStore, currentStateOutput, resourceMap.keySet());

    // Read the best possible assignment from metadata store
    Map<String, ResourceAssignment> currentBestPossibleAssignment =
        _assignmentManager.getBestPossibleAssignment(_assignmentMetadataStore, currentStateOutput,
            resourceMap.keySet());
    ClusterModel clusterModel;
    try {
      clusterModel = ClusterModelProvider
          .generateClusterModelForPartialRebalance(clusterData, resourceMap, activeNodes,
              currentBaseline, currentBestPossibleAssignment);
    } catch (Exception ex) {
      throw new HelixRebalanceException("Failed to generate cluster model for partial rebalance.",
          HelixRebalanceException.Type.INVALID_CLUSTER_STATUS, ex);
    }
    Map<String, ResourceAssignment> newAssignment = WagedRebalanceUtil.calculateAssignment(clusterModel, algorithm);

    // Asynchronously report baseline divergence metric before persisting to metadata store,
    // just in case if persisting fails, we still have the metric.
    // To avoid changes of the new assignment and make it safe when being used to measure baseline
    // divergence, use a deep copy of the new assignment.
    Map<String, ResourceAssignment> newAssignmentCopy = new HashMap<>();
    for (Map.Entry<String, ResourceAssignment> entry : newAssignment.entrySet()) {
      newAssignmentCopy.put(entry.getKey(), new ResourceAssignment(entry.getValue().getRecord()));
    }

    _baselineDivergenceGauge.asyncMeasureAndUpdateValue(clusterData.getAsyncTasksThreadPool(),
        currentBaseline, newAssignmentCopy);

    boolean bestPossibleUpdateSuccessful = false;
    if (_assignmentMetadataStore != null && _assignmentMetadataStore.isBestPossibleChanged(newAssignment)) {
      // This will not persist the new Best Possible Assignment into ZK. It will only update the in-memory cache.
      // If this is done successfully, the new Best Possible Assignment will be persisted into ZK the next time that
      // the pipeline is triggered. We schedule the pipeline to run below.
      bestPossibleUpdateSuccessful = _assignmentMetadataStore.asyncUpdateBestPossibleAssignmentCache(newAssignment,
          newBestPossibleAssignmentVersion);
    } else {
      LOG.debug("Assignment Metadata Store is null. Skip persisting the baseline assignment.");
    }
    _partialRebalanceLatency.endMeasuringLatency();
    LOG.info("Finish calculating the new best possible assignment.");

    if (bestPossibleUpdateSuccessful) {
      LOG.info("Schedule a new rebalance after the new best possible calculation has finished.");
      RebalanceUtil.scheduleOnDemandPipeline(clusterData.getClusterName(), 0L, false);
    }
  }

  public void setPartialRebalanceAsyncMode(boolean isAsyncPartialRebalanceEnabled) {
    _asyncPartialRebalanceEnabled = isAsyncPartialRebalanceEnabled;
  }

  public boolean isAsyncPartialRebalanceEnabled() {
    return _asyncPartialRebalanceEnabled;
  }

  @Override
  public void close() {
    if (_bestPossibleCalculateExecutor != null) {
      _bestPossibleCalculateExecutor.shutdownNow();
    }
  }
}

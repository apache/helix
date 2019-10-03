package org.apache.helix.monitoring.metrics;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import javax.management.JMException;
import org.apache.helix.monitoring.metrics.implementation.RebalanceLatencyGauge;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;

public class WagedRebalancerMetricCollector extends MetricCollector {
  private static final String WAGED_REBALANCER_ENTITY_NAME = "WagedRebalancer";

  /**
   * This enum class contains all metric names defined for WagedRebalancer. Note that all enums are
   * in camel case for readability.
   */
  public enum WagedRebalancerMetricNames {
    // Per-stage latency metrics
    GlobalBaselineCalcLatencyGauge,
    PartialRebalanceLatencyGauge,

    // The following latency metrics are related to AssignmentMetadataStore
    StateReadLatencyGauge,
    StateWriteLatencyGauge
  }

  public WagedRebalancerMetricCollector(String clusterName) throws JMException {
    super(clusterName, WAGED_REBALANCER_ENTITY_NAME);
    createMetrics();
    register();
  }

  /**
   * This constructor will create but will not register metrics. This constructor will be used in
   * case of JMException so that the rebalancer could proceed without registering and emitting
   * metrics.
   */
  public WagedRebalancerMetricCollector() {
    super(null, null);
    createMetrics();
  }

  /**
   * Creates and registers all metrics in MetricCollector for WagedRebalancer.
   */
  private void createMetrics() {
    // Define all metrics
    LatencyMetric globalBaselineCalcLatencyGauge = new RebalanceLatencyGauge(
        WagedRebalancerMetricNames.GlobalBaselineCalcLatencyGauge.name(), getResetIntervalInMs());
    LatencyMetric partialRebalanceLatencyGauge = new RebalanceLatencyGauge(
        WagedRebalancerMetricNames.PartialRebalanceLatencyGauge.name(), getResetIntervalInMs());
    LatencyMetric stateReadLatencyGauge = new RebalanceLatencyGauge(
        WagedRebalancerMetricNames.StateReadLatencyGauge.name(), getResetIntervalInMs());
    LatencyMetric stateWriteLatencyGauge = new RebalanceLatencyGauge(
        WagedRebalancerMetricNames.StateWriteLatencyGauge.name(), getResetIntervalInMs());

    // Add metrics to WagedRebalancerMetricCollector
    addMetric(globalBaselineCalcLatencyGauge);
    addMetric(partialRebalanceLatencyGauge);
    addMetric(stateReadLatencyGauge);
    addMetric(stateWriteLatencyGauge);
  }
}

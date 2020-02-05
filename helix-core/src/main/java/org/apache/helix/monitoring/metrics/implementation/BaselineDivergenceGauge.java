package org.apache.helix.monitoring.metrics.implementation;

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

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.rebalancer.util.ResourceUsageCalculator;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.metrics.model.RatioMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Gauge of the difference (state and partition allocation) between the baseline and the best
 * possible assignment. Its value range is [0.0, 1.0].
 */
public class BaselineDivergenceGauge extends RatioMetric {
  private static final Logger LOG = LoggerFactory.getLogger(BaselineDivergenceGauge.class);

  /**
   * Instantiates a new Simple dynamic metric.
   * @param metricName   the metric name
   */
  public BaselineDivergenceGauge(String metricName) {
    super(metricName, 0.0d);
  }

  /**
   * Asynchronously measure and update metric value.
   * @param threadPool an executor service to asynchronously run the task
   * @param baseline baseline assignment
   * @param bestPossibleAssignment best possible assignment
   */
  public void asyncMeasureAndUpdateValue(ExecutorService threadPool,
      Map<String, ResourceAssignment> baseline,
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    AbstractBaseStage.asyncExecute(threadPool, () -> {
      try {
        double baselineDivergence =
            ResourceUsageCalculator.measureBaselineDivergence(baseline, bestPossibleAssignment);
        updateValue(baselineDivergence);
      } catch (Exception e) {
        LOG.error("Failed to report BaselineDivergenceGauge metric.", e);
      }
      return null;
    });
  }
}

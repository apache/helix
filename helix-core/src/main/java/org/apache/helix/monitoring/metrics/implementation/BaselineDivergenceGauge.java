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

import org.apache.helix.monitoring.metrics.model.RatioMetric;


/**
 * Gauge of the difference (state and partition allocation) between the baseline and the best
 * possible assignment.
 */
public class BaselineDivergenceGauge extends RatioMetric {
  private static final double VALUE_NOT_SET = -1.0d;
  /**
   * Instantiates a new Simple dynamic metric.
   * @param metricName   the metric name
   */
  public BaselineDivergenceGauge(String metricName) {
    super(metricName, VALUE_NOT_SET);
  }
}
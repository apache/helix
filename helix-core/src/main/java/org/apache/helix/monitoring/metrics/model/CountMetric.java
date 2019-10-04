package org.apache.helix.monitoring.metrics.model;

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

import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

/**
 * Represents a count metric and defines methods to help with calculation. A count metric gives a
 * gauge value of a certain property.
 */
public abstract class CountMetric<V> extends SimpleDynamicMetric<V> implements Metric {
  protected V _count;

  /**
   * Instantiates a new Simple dynamic metric.
   * @param metricName the metric name
   * @param metricObject the metric object
   */
  public CountMetric(String metricName, V metricObject) {
    super(metricName, metricObject);
  }

  public abstract void setCount(Object count);
}

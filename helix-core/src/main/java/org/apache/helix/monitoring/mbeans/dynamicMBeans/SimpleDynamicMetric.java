package org.apache.helix.monitoring.mbeans.dynamicMBeans;

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

/**
 * The dynamic metric that accept and emits same type of monitor data
 *
 * @param <T> the type of the metric value
 */
public class SimpleDynamicMetric<T> extends DynamicMetric<T, T> {
  private final String _metricName;

  /**
   * Instantiates a new Simple dynamic metric.
   *
   * @param metricName   the metric name
   * @param metricObject the metric object
   */
  public SimpleDynamicMetric(String metricName, T metricObject) {
    super(metricName, metricObject);
    _metricName = metricName;
  }

  @Override
  public T getAttributeValue(String attributeName) {
    if (!attributeName.equals(_metricName)) {
      return null;
    }
    return getMetricObject();
  }

  /**
   * @return current metric value
   */
  public T getValue() {
    return getMetricObject();
  }

  @Override
  public void updateValue(T metricObject) {
    setMetricObject(metricObject);
  }
}

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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.helix.HelixException;
import org.apache.helix.monitoring.metrics.model.Metric;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;

/**
 * Collects and manages all metrics that implement the {@link Metric} interface.
 */
public abstract class MetricCollector extends DynamicMBeanProvider {
  private static final String CLUSTER_NAME_KEY = "ClusterName";
  private static final String ENTITY_NAME_KEY = "EntityName";
  private String _monitorDomainName;
  private String _clusterName;
  private String _entityName;
  private Map<String, Metric> _metricMap;

  public MetricCollector(String monitorDomainName, String clusterName, String entityName) {
    _monitorDomainName = monitorDomainName;
    _clusterName = clusterName;
    _entityName = entityName;
    _metricMap = new HashMap<>();
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    // First cast all Metric objects to DynamicMetrics
    Collection<DynamicMetric<?, ?>> dynamicMetrics = new HashSet<>();
    _metricMap.values().forEach(metric -> dynamicMetrics.add(metric.getDynamicMetric()));

    // Define MBeanName and ObjectName
    // MBean name has two key-value pairs:
    // ------ 1) ClusterName KV pair (first %s=%s)
    // ------ 2) EntityName KV pair (second %s=%s)
    String mbeanName =
        String.format("%s=%s, %s=%s", CLUSTER_NAME_KEY, _clusterName, ENTITY_NAME_KEY, _entityName);

    // ObjectName has one key-value pair:
    // ------ 1) Monitor domain name KV pair where value is the MBean name
    doRegister(dynamicMetrics,
        new ObjectName(String.format("%s:%s", _monitorDomainName, mbeanName)));
    return this;
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s", MonitorDomainNames.Rebalancer.name(), _clusterName,
        _entityName);
  }

  void addMetric(Metric metric) {
    if (metric instanceof DynamicMetric) {
      _metricMap.putIfAbsent(metric.getMetricName(), metric);
    } else {
      throw new HelixException("MetricCollector only supports Metrics that are DynamicMetric!");
    }
  }

  /**
   * Returns a desired type of the metric.
   * @param metricName
   * @param metricClass Desired type
   * @param <T> Casted result of the metric
   * @return
   */
  public <T extends DynamicMetric> T getMetric(String metricName, Class<T> metricClass) {
    return metricClass.cast(_metricMap.get(metricName));
  }

  public Map<String, Metric> getMetricMap() {
    return _metricMap;
  }
}

package org.apache.helix.monitoring.mbeans;

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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.JMException;
import javax.management.ObjectName;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;


/**
 * Implementation of the instance status bean
 */
public class InstanceMonitor extends DynamicMBeanProvider {
  /**
   * Metric names for instance capacity.
   */
  public enum InstanceMonitorMetrics {
    TOTAL_MESSAGE_RECEIVED_COUNTER("TotalMessageReceivedCounter"),
    ENABLED_STATUS_GAUGE("EnabledStatusGauge"),
    ONLINE_STATUS_GAUGE("OnlineStatusGauge"),
    MAX_CAPACITY_USAGE_GAUGE("MaxCapacityUsageGauge"),
    DISABLED_PARTITIONS_GAUGE("DisabledPartitionsGauge");

    private String metricName;

    InstanceMonitorMetrics(String name) {
      metricName = name;
    }

    public String metricName() {
      return metricName;
    }
  }

  private final String _clusterName;
  private final String _participantName;
  private final ObjectName _initObjectName;

  private List<String> _tags;

  // Counters
  private SimpleDynamicMetric<Long> _totalMessagedReceivedCounter;

  // Gauges
  private SimpleDynamicMetric<Long> _enabledStatusGauge;
  private SimpleDynamicMetric<Long> _disabledPartitionsGauge;
  private SimpleDynamicMetric<Long> _onlineStatusGauge;
  private SimpleDynamicMetric<Double> _maxCapacityUsageGauge;

  /**
   * Initialize the bean
   * @param clusterName the cluster to monitor
   * @param participantName the instance whose statistics this holds
   */
  public InstanceMonitor(String clusterName, String participantName, ObjectName objectName)
      throws JMException {
    _clusterName = clusterName;
    _participantName = participantName;
    _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    _initObjectName = objectName;

    createMetrics();
  }

  private void createMetrics() {
    _totalMessagedReceivedCounter = new SimpleDynamicMetric<>(
        InstanceMonitorMetrics.TOTAL_MESSAGE_RECEIVED_COUNTER.metricName(), 0L);

    _disabledPartitionsGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetrics.DISABLED_PARTITIONS_GAUGE.metricName(),
            0L);
    _enabledStatusGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetrics.ENABLED_STATUS_GAUGE.metricName(), 0L);
    _onlineStatusGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetrics.ONLINE_STATUS_GAUGE.metricName(), 0L);
    _maxCapacityUsageGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetrics.MAX_CAPACITY_USAGE_GAUGE.metricName(),
            0.0d);
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s.%s", ClusterStatusMonitor.PARTICIPANT_STATUS_KEY, _clusterName,
        serializedTags(), _participantName);
  }

  public long getOnline() {
    return _onlineStatusGauge.getValue();
  }

  public long getEnabled() {
    return _enabledStatusGauge.getValue();
  }

  public long getTotalMessageReceived() {
    return _totalMessagedReceivedCounter.getValue();
  }

  public long getDisabledPartitions() {
    return _disabledPartitionsGauge.getValue();
  }

  /**
   * Get all the tags currently on this instance
   * @return list of tags
   */
  public List<String> getTags() {
    return _tags;
  }

  /**
   * Get the name of the monitored instance
   * @return instance name as a string
   */
  public String getInstanceName() {
    return _participantName;
  }

  private String serializedTags() {
    return Joiner.on('|').skipNulls().join(_tags);
  }

  /**
   * Update the gauges for this instance
   * @param tags current tags
   * @param disabledPartitions current disabled partitions
   * @param isLive true if running, false otherwise
   * @param isEnabled true if enabled, false if disabled
   */
  public synchronized void updateInstance(Set<String> tags,
      Map<String, List<String>> disabledPartitions, List<String> oldDisabledPartitions,
      boolean isLive, boolean isEnabled) {
    if (tags == null || tags.isEmpty()) {
      _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    } else {
      _tags = Lists.newArrayList(tags);
      Collections.sort(_tags);
    }
    long numDisabledPartitions = 0L;
    if (disabledPartitions != null) {
      for (List<String> partitions : disabledPartitions.values()) {
        if (partitions != null) {
          numDisabledPartitions += partitions.size();
        }
      }
    }
    // TODO : Get rid of this when old API removed.
    if (oldDisabledPartitions != null) {
      numDisabledPartitions += oldDisabledPartitions.size();
    }

    _onlineStatusGauge.updateValue(isLive ? 1L : 0L);
    _enabledStatusGauge.updateValue(isEnabled ? 1L : 0L);
    _disabledPartitionsGauge.updateValue(numDisabledPartitions);
  }

  /**
   * Increase message received for this instance
   * @param messageReceived received message numbers
   */
  public synchronized void increaseMessageCount(long messageReceived) {
    _totalMessagedReceivedCounter
        .updateValue(_totalMessagedReceivedCounter.getValue() + messageReceived);
  }

  /**
   * Update max capacity usage for this instance.
   * @param maxCapacityUsage max capacity usage of this instance
   */
  public synchronized void updateMaxUsage(double maxCapacityUsage) {
    _maxCapacityUsageGauge.updateValue(maxCapacityUsage);
  }

  /**
   * Get max capacity usage of this instance.
   * @return Max capacity usage of this instance.
   */
  protected synchronized double getMaxCapacityUsageGauge() {
    return _maxCapacityUsageGauge.getValue();
  }

  @Override
  public DynamicMBeanProvider register()
      throws JMException {
    List<DynamicMetric<?, ?>> attributeList = ImmutableList.of(
        _totalMessagedReceivedCounter,
        _disabledPartitionsGauge,
        _enabledStatusGauge,
        _onlineStatusGauge,
        _maxCapacityUsageGauge
    );

    doRegister(attributeList, _initObjectName);

    return this;
  }
}

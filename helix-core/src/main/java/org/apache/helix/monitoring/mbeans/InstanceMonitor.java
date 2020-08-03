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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.ConcurrentHashMap;
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
  public enum InstanceMonitorMetric {
    // TODO: change the metric names with Counter and Gauge suffix and deprecate old names.
    TOTAL_MESSAGE_RECEIVED_COUNTER("TotalMessageReceived"),
    ENABLED_STATUS_GAUGE("Enabled"),
    ONLINE_STATUS_GAUGE("Online"),
    DISABLED_PARTITIONS_GAUGE("DisabledPartitions"),
    MAX_CAPACITY_USAGE_GAUGE("MaxCapacityUsageGauge"),
    MESSAGE_QUEUE_SIZE_GAUGE("MessageQueueSizeGauge"),
    PASTDUE_MESSAGE_GAUGE("PastDueMessageGauge");

    private final String metricName;

    InstanceMonitorMetric(String name) {
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
  private SimpleDynamicMetric<Long> _messageQueueSizeGauge;
  private SimpleDynamicMetric<Long> _pastDueMessageGauge;

  // A map of dynamic capacity Gauges. The map's keys could change.
  private final Map<String, SimpleDynamicMetric<Long>> _dynamicCapacityMetricsMap;

  /**
   * Initialize the bean
   * @param clusterName the cluster to monitor
   * @param participantName the instance whose statistics this holds
   */
  public InstanceMonitor(String clusterName, String participantName, ObjectName objectName) {
    _clusterName = clusterName;
    _participantName = participantName;
    _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    _initObjectName = objectName;
    _dynamicCapacityMetricsMap = new ConcurrentHashMap<>();

    createMetrics();
  }

  private void createMetrics() {
    _totalMessagedReceivedCounter = new SimpleDynamicMetric<>(
        InstanceMonitorMetric.TOTAL_MESSAGE_RECEIVED_COUNTER.metricName(), 0L);

    _disabledPartitionsGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetric.DISABLED_PARTITIONS_GAUGE.metricName(),
            0L);
    _enabledStatusGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetric.ENABLED_STATUS_GAUGE.metricName(), 0L);
    _onlineStatusGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetric.ONLINE_STATUS_GAUGE.metricName(), 0L);
    _maxCapacityUsageGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetric.MAX_CAPACITY_USAGE_GAUGE.metricName(),
            0.0d);
    _messageQueueSizeGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetric.MESSAGE_QUEUE_SIZE_GAUGE.metricName(),
            0L);
    _pastDueMessageGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetric.PASTDUE_MESSAGE_GAUGE.metricName(),
            0L);
  }

  private List<DynamicMetric<?, ?>> buildAttributeList() {
    List<DynamicMetric<?, ?>> attributeList = Lists.newArrayList(
        _totalMessagedReceivedCounter,
        _disabledPartitionsGauge,
        _enabledStatusGauge,
        _onlineStatusGauge,
        _maxCapacityUsageGauge,
        _messageQueueSizeGauge,
        _pastDueMessageGauge
    );

    attributeList.addAll(_dynamicCapacityMetricsMap.values());

    return attributeList;
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s.%s", ClusterStatusMonitor.PARTICIPANT_STATUS_KEY, _clusterName,
        serializedTags(), _participantName);
  }

  protected long getOnline() {
    return _onlineStatusGauge.getValue();
  }

  protected long getEnabled() {
    return _enabledStatusGauge.getValue();
  }

  protected long getTotalMessageReceived() {
    return _totalMessagedReceivedCounter.getValue();
  }

  protected long getDisabledPartitions() {
    return _disabledPartitionsGauge.getValue();
  }

  protected long getMessageQueueSizeGauge() { return _messageQueueSizeGauge.getValue(); }

  protected long getPastDueMessageGauge() { return _pastDueMessageGauge.getValue(); }

  /**
   * Get the name of the monitored instance
   * @return instance name as a string
   */
  protected String getInstanceName() {
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
   * Updates max capacity usage for this instance.
   * @param maxUsage max capacity usage of this instance
   */
  public synchronized void updateMaxCapacityUsage(double maxUsage) {
    _maxCapacityUsageGauge.updateValue(maxUsage);
  }

  /**
   * Updates message queue size for this instance.
   * @param queueSize message queue size of this instance
   */
  public synchronized void updateMessageQueueSize(long queueSize) {
    _messageQueueSizeGauge.updateValue(queueSize);
  }

  /**
   * Updates number of messages that has not been completed after its expected completion time for this instance.
   * @param msgCount count of messages that has not been completed after its due completion time
   */
  public synchronized void updatePastDueMessageGauge(long msgCount) {
    _pastDueMessageGauge.updateValue(msgCount);
  }

  /**
   * Gets max capacity usage of this instance.
   * @return Max capacity usage of this instance.
   */
  protected synchronized double getMaxCapacityUsageGauge() {
    return _maxCapacityUsageGauge.getValue();
  }

  /**
   * Updates instance capacity metrics.
   * @param capacity A map of instance capacity.
   */
  public void updateCapacity(Map<String, Integer> capacity) {
    synchronized (_dynamicCapacityMetricsMap) {
      // If capacity keys don't have any change, we just update the metric values.
      if (_dynamicCapacityMetricsMap.keySet().equals(capacity.keySet())) {
        for (Map.Entry<String, Integer> entry : capacity.entrySet()) {
          _dynamicCapacityMetricsMap.get(entry.getKey()).updateValue((long) entry.getValue());
        }
        return;
      }

      // If capacity keys have any changes, we need to retain the capacity metrics.
      // Make sure capacity metrics map has the same capacity keys.
      // And update metrics values.
      _dynamicCapacityMetricsMap.keySet().retainAll(capacity.keySet());
      for (Map.Entry<String, Integer> entry : capacity.entrySet()) {
        String capacityName = entry.getKey();
        if (_dynamicCapacityMetricsMap.containsKey(capacityName)) {
          _dynamicCapacityMetricsMap.get(capacityName).updateValue((long) entry.getValue());
        } else {
          _dynamicCapacityMetricsMap.put(capacityName,
              new SimpleDynamicMetric<>(capacityName + "Gauge", (long) entry.getValue()));
        }
      }
    }

    // Update MBean's all attributes.
    updateAttributesInfo(buildAttributeList(),
        "Instance monitor for instance: " + getInstanceName());
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    doRegister(buildAttributeList(), _initObjectName);

    return this;
  }
}

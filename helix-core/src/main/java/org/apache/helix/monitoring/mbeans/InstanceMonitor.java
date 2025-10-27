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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.ObjectName;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.helix.constants.InstanceConstants;
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
    ALL_PARTITIONS_DISABLED_GAUGE("AllPartitionsDisabled"),
    MAX_CAPACITY_USAGE_GAUGE("MaxCapacityUsageGauge"),
    MESSAGE_QUEUE_SIZE_GAUGE("MessageQueueSizeGauge"),
    PASTDUE_MESSAGE_GAUGE("PastDueMessageGauge"),
    INSTANCE_OPERATION_DURATION_ENABLE_GAUGE("InstanceOperationDuration_ENABLE"),
    INSTANCE_OPERATION_DURATION_DISABLE_GAUGE("InstanceOperationDuration_DISABLE"),
    INSTANCE_OPERATION_DURATION_EVACUATE_GAUGE("InstanceOperationDuration_EVACUATE"),
    INSTANCE_OPERATION_DURATION_SWAP_IN_GAUGE("InstanceOperationDuration_SWAP_IN"),
    INSTANCE_OPERATION_DURATION_UNKNOWN_GAUGE("InstanceOperationDuration_UNKNOWN");

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
  private SimpleDynamicMetric<Long> _allPartitionsDisabledGauge;
  private SimpleDynamicMetric<Long> _onlineStatusGauge;
  private SimpleDynamicMetric<Double> _maxCapacityUsageGauge;
  private SimpleDynamicMetric<Long> _messageQueueSizeGauge;
  private SimpleDynamicMetric<Long> _pastDueMessageGauge;

  // Instance Operation Duration Gauges (in milliseconds)
  private SimpleDynamicMetric<Long> _instanceOperationDurationEnableGauge;
  private SimpleDynamicMetric<Long> _instanceOperationDurationDisableGauge;
  private SimpleDynamicMetric<Long> _instanceOperationDurationEvacuateGauge;
  private SimpleDynamicMetric<Long> _instanceOperationDurationSwapInGauge;
  private SimpleDynamicMetric<Long> _instanceOperationDurationUnknownGauge;

  // Track current operation and start time for duration calculation
  private InstanceConstants.InstanceOperation _currentOperation;
  private long _currentOperationStartTime;

  // A map of dynamic capacity Gauges. The map's keys could change.
  private final Map<String, SimpleDynamicMetric<Long>> _dynamicCapacityMetricsMap;

  // Background executor for resetting gauges
  private final ScheduledExecutorService _resetExecutor;

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
    _currentOperation = InstanceConstants.InstanceOperation.ENABLE;
    // Initialize to 0 so that if we haven't received operation info yet, duration will be large
    // and will be corrected when we receive the actual operation start time from InstanceConfig
    _currentOperationStartTime = 0L;
    _resetExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread thread = new Thread(r, "InstanceMonitor-ResetGauges-" + participantName);
      thread.setDaemon(true);
      return thread;
    });

    createMetrics();
  }

  private void createMetrics() {
    _totalMessagedReceivedCounter = new SimpleDynamicMetric<>(
        InstanceMonitorMetric.TOTAL_MESSAGE_RECEIVED_COUNTER.metricName(), 0L);

    _disabledPartitionsGauge =
        new SimpleDynamicMetric<>(InstanceMonitorMetric.DISABLED_PARTITIONS_GAUGE.metricName(),
            0L);
    _allPartitionsDisabledGauge = new SimpleDynamicMetric<>(
        InstanceMonitorMetric.ALL_PARTITIONS_DISABLED_GAUGE.metricName(), 0L);
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

    // Initialize instance operation duration gauges
    _instanceOperationDurationEnableGauge = new SimpleDynamicMetric<>(
        InstanceMonitorMetric.INSTANCE_OPERATION_DURATION_ENABLE_GAUGE.metricName(), 0L);
    _instanceOperationDurationDisableGauge = new SimpleDynamicMetric<>(
        InstanceMonitorMetric.INSTANCE_OPERATION_DURATION_DISABLE_GAUGE.metricName(), 0L);
    _instanceOperationDurationEvacuateGauge = new SimpleDynamicMetric<>(
        InstanceMonitorMetric.INSTANCE_OPERATION_DURATION_EVACUATE_GAUGE.metricName(), 0L);
    _instanceOperationDurationSwapInGauge = new SimpleDynamicMetric<>(
        InstanceMonitorMetric.INSTANCE_OPERATION_DURATION_SWAP_IN_GAUGE.metricName(), 0L);
    _instanceOperationDurationUnknownGauge = new SimpleDynamicMetric<>(
        InstanceMonitorMetric.INSTANCE_OPERATION_DURATION_UNKNOWN_GAUGE.metricName(), 0L);
  }

  private List<DynamicMetric<?, ?>> buildAttributeList() {
    List<DynamicMetric<?, ?>> attributeList = Lists.newArrayList(
        _totalMessagedReceivedCounter,
        _disabledPartitionsGauge,
        _allPartitionsDisabledGauge,
        _enabledStatusGauge,
        _onlineStatusGauge,
        _maxCapacityUsageGauge,
        _messageQueueSizeGauge,
        _pastDueMessageGauge,
        _instanceOperationDurationEnableGauge,
        _instanceOperationDurationDisableGauge,
        _instanceOperationDurationEvacuateGauge,
        _instanceOperationDurationSwapInGauge,
        _instanceOperationDurationUnknownGauge
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
  protected long getAllPartitionsDisabled() { return _allPartitionsDisabledGauge.getValue(); }

  protected long getMessageQueueSizeGauge() { return _messageQueueSizeGauge.getValue(); }

  protected long getPastDueMessageGauge() { return _pastDueMessageGauge.getValue(); }

  /**
   * Get the current duration in ENABLE operation (in milliseconds)
   * @return duration in milliseconds, or 0 if not in ENABLE state
   */
  protected long getInstanceOperationDurationEnable() {
    return _instanceOperationDurationEnableGauge.getValue();
  }

  /**
   * Get the current duration in DISABLE operation (in milliseconds)
   * @return duration in milliseconds, or 0 if not in DISABLE state
   */
  protected long getInstanceOperationDurationDisable() {
    return _instanceOperationDurationDisableGauge.getValue();
  }

  /**
   * Get the current duration in EVACUATE operation (in milliseconds)
   * @return duration in milliseconds, or 0 if not in EVACUATE state
   */
  protected long getInstanceOperationDurationEvacuate() {
    return _instanceOperationDurationEvacuateGauge.getValue();
  }

  /**
   * Get the current duration in SWAP_IN operation (in milliseconds)
   * @return duration in milliseconds, or 0 if not in SWAP_IN state
   */
  protected long getInstanceOperationDurationSwapIn() {
    return _instanceOperationDurationSwapInGauge.getValue();
  }

  /**
   * Get the current duration in UNKNOWN operation (in milliseconds)
   * @return duration in milliseconds, or 0 if not in UNKNOWN state
   */
  protected long getInstanceOperationDurationUnknown() {
    return _instanceOperationDurationUnknownGauge.getValue();
  }

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
    boolean allPartitionsDisabled = false;
    if (disabledPartitions != null) {
      for (List<String> partitions : disabledPartitions.values()) {
        if (partitions != null) {
          numDisabledPartitions += partitions.size();
          if (partitions.contains(InstanceConstants.ALL_RESOURCES_DISABLED_PARTITION_KEY)) {
            allPartitionsDisabled = true;
            numDisabledPartitions -= 1;
          }
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
    _allPartitionsDisabledGauge.updateValue(allPartitionsDisabled ? 1L : 0L);
  }

  /**
   * Updates the duration gauge for a specific operation.
   * @param operation the operation to update
   * @param duration the duration value to set
   */
  private void updateOperationDurationGauge(InstanceConstants.InstanceOperation operation, long duration) {
    switch (operation) {
      case ENABLE:
        _instanceOperationDurationEnableGauge.updateValue(duration);
        break;
      case DISABLE:
        _instanceOperationDurationDisableGauge.updateValue(duration);
        break;
      case EVACUATE:
        _instanceOperationDurationEvacuateGauge.updateValue(duration);
        break;
      case SWAP_IN:
        _instanceOperationDurationSwapInGauge.updateValue(duration);
        break;
      case UNKNOWN:
        _instanceOperationDurationUnknownGauge.updateValue(duration);
        break;
      default:
        // Should not happen, but handle gracefully
        _instanceOperationDurationUnknownGauge.updateValue(duration);
        break;
    }
  }

  /**
   * Resets all operation duration gauges except the specified operations to 0.
   * This method is typically called after a delay to ensure only the specified
   * operations show a non-zero duration.
   *
   * @param operationsToExclude the set of operations to exclude from reset
   */
  private void resetAllExceptOperations(Set<InstanceConstants.InstanceOperation> operationsToExclude) {
    // Reset all gauges except the ones for the excluded operations
    for (InstanceConstants.InstanceOperation operation : InstanceConstants.InstanceOperation.values()) {
      if (!operationsToExclude.contains(operation)) {
        updateOperationDurationGauge(operation, 0L);
      }
    }
  }

  /**
   * Update the instance operation and recalculate duration metrics.
   * This method should be called whenever the instance operation changes or periodically
   * to update the duration of the current operation.
   *
   * @param newOperation the current instance operation
   * @param operationStartTime the timestamp (in milliseconds since epoch) when the operation started.
   *                          This should come from InstanceConfig.InstanceOperation.getTimestamp()
   *                          to survive controller restarts. Use -1 if timestamp is unknown
   *                          (for backward compatibility with legacy HELIX_ENABLED field).
   */
  public synchronized void updateInstanceOperation(InstanceConstants.InstanceOperation newOperation,
      long operationStartTime) {
    if (newOperation == null) {
      newOperation = InstanceConstants.InstanceOperation.ENABLE;
    }

    // Handle backward compatibility: if timestamp is -1 (unknown), use current time
    // This happens when InstanceOperation is not set and we're using legacy HELIX_ENABLED field
    // We capture current time ONCE to ensure consistency across calculations
    long currentTime = System.currentTimeMillis();
    if (operationStartTime == -1L) {
      operationStartTime = currentTime;
    }

    // Check if operation changed
    if (_currentOperation != newOperation) {
      // Only update final duration if we had a valid start time
      // On first call after controller restart, _currentOperationStartTime may be 0
      if (_currentOperationStartTime > 0L) {
        long finalDuration = currentTime - _currentOperationStartTime;
        updateOperationDurationGauge(_currentOperation, finalDuration);
      }

      // Now switch to the new operation
      _currentOperation = newOperation;
      _currentOperationStartTime = operationStartTime;
    }

    // Update the duration gauge for the current operation
    long currentDuration = currentTime - _currentOperationStartTime;
    updateOperationDurationGauge(_currentOperation, currentDuration);

    // Capture the current operation at scheduling time to avoid race conditions
    // If we transition from ENABLE -> EVACUATE -> ENABLE within 2 minutes,
    // we want to reset based on what the operation was when we scheduled the task,
    // not what it becomes later.
    final InstanceConstants.InstanceOperation operationToExclude = _currentOperation;

    // Schedule a background task to reset all gauges except the captured operation after 2 minutes
    _resetExecutor.schedule(() -> {
      synchronized (InstanceMonitor.this) {
        resetAllExceptOperations(Collections.singleton(operationToExclude));
      }
    }, 2, TimeUnit.MINUTES);
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

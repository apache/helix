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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

import javax.management.JMException;
import javax.management.ObjectName;
import java.util.*;

public class ResourceMonitor extends DynamicMBeanProvider {

  // Gauges
  private SimpleDynamicMetric<Integer> _numOfPartitions;
  private SimpleDynamicMetric<Integer> _numOfPartitionsInExternalView;
  private SimpleDynamicMetric<Integer> _numOfErrorPartitions;
  private SimpleDynamicMetric<Integer> _numNonTopStatePartitions;
  private SimpleDynamicMetric<Long> _numLessMinActiveReplicaPartitions;
  private SimpleDynamicMetric<Long> _numLessReplicaPartitions;
  private SimpleDynamicMetric<Long> _numPendingRecoveryRebalancePartitions;
  private SimpleDynamicMetric<Long> _numPendingLoadRebalancePartitions;
  private SimpleDynamicMetric<Long> _numRecoveryRebalanceThrottledPartitions;
  private SimpleDynamicMetric<Long> _numLoadRebalanceThrottledPartitions;
  private SimpleDynamicMetric<Integer> _externalViewIdealStateDiff;

  // Counters
  private SimpleDynamicMetric<Long> _successfulTopStateHandoffDurationCounter;
  private SimpleDynamicMetric<Long> _successTopStateHandoffCounter;
  private SimpleDynamicMetric<Long> _failedTopStateHandoffCounter;
  private SimpleDynamicMetric<Long> _maxSinglePartitionTopStateHandoffDuration;
  private HistogramDynamicMetric _partitionTopStateHandoffDurationGauge;
  private SimpleDynamicMetric<Long> _totalMessageReceived;

  private String _tag = ClusterStatusMonitor.DEFAULT_TAG;
  private long _lastResetTime;
  private final String _resourceName;
  private final String _clusterName;
  private final ObjectName _initObjectName;

  @Override
  public ResourceMonitor register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_numOfPartitions);
    attributeList.add(_numOfPartitionsInExternalView);
    attributeList.add(_numOfErrorPartitions);
    attributeList.add(_numNonTopStatePartitions);
    attributeList.add(_numLessMinActiveReplicaPartitions);
    attributeList.add(_numLessReplicaPartitions);
    attributeList.add(_numPendingRecoveryRebalancePartitions);
    attributeList.add(_numPendingLoadRebalancePartitions);
    attributeList.add(_numRecoveryRebalanceThrottledPartitions);
    attributeList.add(_numLoadRebalanceThrottledPartitions);
    attributeList.add(_externalViewIdealStateDiff);
    attributeList.add(_successfulTopStateHandoffDurationCounter);
    attributeList.add(_successTopStateHandoffCounter);
    attributeList.add(_failedTopStateHandoffCounter);
    attributeList.add(_maxSinglePartitionTopStateHandoffDuration);
    attributeList.add(_partitionTopStateHandoffDurationGauge);
    attributeList.add(_totalMessageReceived);
    doRegister(attributeList, _initObjectName);

    return this;
  }

  public enum MonitorState {
    TOP_STATE
  }

  public ResourceMonitor(String clusterName, String resourceName, ObjectName objectName)
      throws JMException {
    _clusterName = clusterName;
    _resourceName = resourceName;
    _initObjectName = objectName;

    _externalViewIdealStateDiff = new SimpleDynamicMetric("DifferenceWithIdealStateGauge", 0l);
    _numLoadRebalanceThrottledPartitions =
        new SimpleDynamicMetric("LoadRebalanceThrottledPartitionGauge", 0l);
    _numRecoveryRebalanceThrottledPartitions =
        new SimpleDynamicMetric("RecoveryRebalanceThrottledPartitionGauge", 0l);
    _numPendingLoadRebalancePartitions =
        new SimpleDynamicMetric("PendingLoadRebalancePartitionGauge", 0l);
    _numPendingRecoveryRebalancePartitions =
        new SimpleDynamicMetric("PendingRecoveryRebalancePartitionGauge", 0l);
    _numLessReplicaPartitions = new SimpleDynamicMetric("MissingReplicaPartitionGauge", 0l);
    _numLessMinActiveReplicaPartitions =
        new SimpleDynamicMetric("MissingMinActiveReplicaPartitionGauge", 0l);
    _numNonTopStatePartitions = new SimpleDynamicMetric("MissingTopStatePartitionGauge", 0l);
    _numOfErrorPartitions = new SimpleDynamicMetric("ErrorPartitionGauge", 0l);
    _numOfPartitionsInExternalView = new SimpleDynamicMetric("ExternalViewPartitionGauge", 0l);
    _numOfPartitions = new SimpleDynamicMetric("PartitionGauge", 0l);

    _partitionTopStateHandoffDurationGauge =
        new HistogramDynamicMetric("PartitionTopStateHandoffDurationGauge", new Histogram(
            new SlidingTimeWindowArrayReservoir(DEFAULT_RESET_INTERVAL_MS, TimeUnit.MILLISECONDS)));
    _totalMessageReceived = new SimpleDynamicMetric("TotalMessageReceived", 0l);
    _maxSinglePartitionTopStateHandoffDuration =
        new SimpleDynamicMetric("MaxSinglePartitionTopStateHandoffDurationGauge", 0l);
    _failedTopStateHandoffCounter = new SimpleDynamicMetric("FailedTopStateHandoffCounter", 0l);
    _successTopStateHandoffCounter = new SimpleDynamicMetric("SucceededTopStateHandoffCounter", 0l);
    _successfulTopStateHandoffDurationCounter =
        new SimpleDynamicMetric("SuccessfulTopStateHandoffDurationCounter", 0l);
  }

  @Override
  public String getSensorName() {
    return String
        .format("%s.%s.%s.%s", ClusterStatusMonitor.RESOURCE_STATUS_KEY, _clusterName, _tag,
            _resourceName);
  }

  public long getPartitionGauge() {
    return _numOfPartitions.getValue();
  }

  public long getErrorPartitionGauge() {
    return _numOfErrorPartitions.getValue();
  }

  public long getMissingTopStatePartitionGauge() {
    return _numNonTopStatePartitions.getValue();
  }

  public long getDifferenceWithIdealStateGauge() {
    return _externalViewIdealStateDiff.getValue();
  }

  public long getSuccessfulTopStateHandoffDurationCounter() {
    return _successfulTopStateHandoffDurationCounter.getValue();
  }

  public long getSucceededTopStateHandoffCounter() {
    return _successTopStateHandoffCounter.getValue();
  }

  public long getMaxSinglePartitionTopStateHandoffDurationGauge() {
    return _maxSinglePartitionTopStateHandoffDuration.getValue();
  }

  public long getFailedTopStateHandoffCounter() {
    return _failedTopStateHandoffCounter.getValue();
  }

  public long getTotalMessageReceived() {
    return _totalMessageReceived.getValue();
  }

  public synchronized void increaseMessageCount(long messageReceived) {
    _totalMessageReceived.updateValue(_totalMessageReceived.getValue() + messageReceived);
  }

  public String getResourceName() {
    return _resourceName;
  }

  public String getBeanName() {
    return _clusterName + " " + _resourceName;
  }

  public void updateResource(ExternalView externalView, IdealState idealState,
      StateModelDefinition stateModelDef) {
    if (externalView == null) {
      _logger.warn("External view is null");
      return;
    }

    String topState = null;
    if (stateModelDef != null) {
      List<String> priorityList = stateModelDef.getStatesPriorityList();
      if (!priorityList.isEmpty()) {
        topState = priorityList.get(0);
      }
    }

    resetGauges();
    if (idealState == null) {
      _logger.warn("ideal state is null for " + _resourceName);
      return;
    }

    assert (_resourceName.equals(idealState.getId()));
    assert (_resourceName.equals(externalView.getId()));

    int numOfErrorPartitions = 0;
    int numOfDiff = 0;
    int numOfPartitionWithTopState = 0;

    Set<String> partitions = idealState.getPartitionSet();

    _numOfPartitions.updateValue(partitions.size());

    int replica = -1;
    try {
      replica = Integer.valueOf(idealState.getReplicas());
    } catch (NumberFormatException e) {
    }

    int minActiveReplica = idealState.getMinActiveReplicas();
    minActiveReplica = (minActiveReplica >= 0) ? minActiveReplica : replica;

    for (String partition : partitions) {
      Map<String, String> idealRecord = idealState.getInstanceStateMap(partition);
      Map<String, String> externalViewRecord = externalView.getStateMap(partition);

      if (idealRecord == null) {
        idealRecord = Collections.emptyMap();
      }
      if (externalViewRecord == null) {
        externalViewRecord = Collections.emptyMap();
      }
      if (!idealRecord.entrySet().equals(externalViewRecord.entrySet())) {
        numOfDiff++;
      }

      int activeReplicaCount = 0;
      boolean hasTopState = false;
      for (String host : externalViewRecord.keySet()) {
        String currentState = externalViewRecord.get(host);
        if (HelixDefinedState.ERROR.toString().equalsIgnoreCase(currentState)) {
          numOfErrorPartitions++;
        }
        if (topState != null && topState.equalsIgnoreCase(currentState)) {
          hasTopState = true;
        }

        Map<String, Integer> stateCount =
            stateModelDef.getStateCountMap(idealRecord.size(), replica);
        Set<String> activeStates = stateCount.keySet();
        if (currentState != null && activeStates.contains(currentState)) {
          activeReplicaCount++;
        }
      }
      if (hasTopState) {
        numOfPartitionWithTopState ++;
      }
      if (replica > 0 && activeReplicaCount < replica) {
        _numLessReplicaPartitions.updateValue(_numLessReplicaPartitions.getValue() + 1);
      }
      if (minActiveReplica >= 0 && activeReplicaCount < minActiveReplica) {
        _numLessMinActiveReplicaPartitions
            .updateValue(_numLessMinActiveReplicaPartitions.getValue() + 1);
      }
    }
    _numOfErrorPartitions.updateValue(numOfErrorPartitions);
    _externalViewIdealStateDiff.updateValue(numOfDiff);
    _numOfPartitionsInExternalView.updateValue(externalView.getPartitionSet().size());
    _numNonTopStatePartitions.updateValue(_numOfPartitions.getValue() - numOfPartitionWithTopState);

    String tag = idealState.getInstanceGroupTag();
    if (tag == null || tag.equals("") || tag.equals("null")) {
      _tag = ClusterStatusMonitor.DEFAULT_TAG;
    } else {
      _tag = tag;
    }
  }

  private void resetGauges() {
    _numOfErrorPartitions.updateValue(0);
    _numNonTopStatePartitions.updateValue(0);
    _externalViewIdealStateDiff.updateValue(0);
    _numOfPartitionsInExternalView.updateValue(0);
    _numLessMinActiveReplicaPartitions.updateValue(0l);
    _numLessReplicaPartitions.updateValue(0l);
    _numPendingRecoveryRebalancePartitions.updateValue(0l);
    _numPendingLoadRebalancePartitions.updateValue(0l);
    _numRecoveryRebalanceThrottledPartitions.updateValue(0l);
    _numLoadRebalanceThrottledPartitions.updateValue(0l);
  }

  public void updateStateHandoffStats(MonitorState monitorState, long duration, boolean succeeded) {
    switch (monitorState) {
    case TOP_STATE:
      if (succeeded) {
        _successTopStateHandoffCounter.updateValue(_successTopStateHandoffCounter.getValue() + 1);
        _successfulTopStateHandoffDurationCounter
            .updateValue(_successfulTopStateHandoffDurationCounter.getValue() + duration);
        _partitionTopStateHandoffDurationGauge.updateValue(duration);
        if (duration > _maxSinglePartitionTopStateHandoffDuration.getValue()) {
          _maxSinglePartitionTopStateHandoffDuration.updateValue(duration);
          _lastResetTime = System.currentTimeMillis();
        }
      } else {
        _failedTopStateHandoffCounter.updateValue(_failedTopStateHandoffCounter.getValue() + 1);
      }
      break;
    default:
      _logger.warn(
          String.format("Wrong monitor state \"%s\" that not supported ", monitorState.name()));
    }
  }

  public void updateRebalancerStat(long numPendingRecoveryRebalancePartitions,
      long numPendingLoadRebalancePartitions, long numRecoveryRebalanceThrottledPartitions,
      long numLoadRebalanceThrottledPartitions) {
    _numPendingRecoveryRebalancePartitions.updateValue(numPendingRecoveryRebalancePartitions);
    _numPendingLoadRebalancePartitions.updateValue(numPendingLoadRebalancePartitions);
    _numRecoveryRebalanceThrottledPartitions.updateValue(numRecoveryRebalanceThrottledPartitions);
    _numLoadRebalanceThrottledPartitions.updateValue(numLoadRebalanceThrottledPartitions);
  }

  public long getExternalViewPartitionGauge() {
    return _numOfPartitionsInExternalView.getValue();
  }

  public long getMissingMinActiveReplicaPartitionGauge() {
    return _numLessMinActiveReplicaPartitions.getValue();
  }

  public long getMissingReplicaPartitionGauge() {
    return _numLessReplicaPartitions.getValue();
  }

  public long getPendingRecoveryRebalancePartitionGauge() {
    return _numPendingRecoveryRebalancePartitions.getValue();
  }

  public long getPendingLoadRebalancePartitionGauge() {
    return _numPendingLoadRebalancePartitions.getValue();
  }

  public long getRecoveryRebalanceThrottledPartitionGauge() {
    return _numRecoveryRebalanceThrottledPartitions.getValue();
  }

  public long getLoadRebalanceThrottledPartitionGauge() {
    return _numLoadRebalanceThrottledPartitions.getValue();
  }

  public void resetMaxTopStateHandoffGauge() {
    if (_lastResetTime + DEFAULT_RESET_INTERVAL_MS <= System.currentTimeMillis()) {
      _maxSinglePartitionTopStateHandoffDuration.updateValue(0l);
      _lastResetTime = System.currentTimeMillis();
    }
  }
}

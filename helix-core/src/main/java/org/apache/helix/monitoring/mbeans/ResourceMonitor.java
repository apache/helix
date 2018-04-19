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
  private SimpleDynamicMetric<Long> _numOfPartitions;
  private SimpleDynamicMetric<Long> _numOfPartitionsInExternalView;
  private SimpleDynamicMetric<Long> _numOfErrorPartitions;
  private SimpleDynamicMetric<Long> _numNonTopStatePartitions;
  private SimpleDynamicMetric<Long> _externalViewIdealStateDiff;
  private SimpleDynamicMetric<Long> _numLessMinActiveReplicaPartitions;
  private SimpleDynamicMetric<Long> _numLessReplicaPartitions;
  private SimpleDynamicMetric<Long> _numPendingRecoveryRebalancePartitions;
  private SimpleDynamicMetric<Long> _numPendingLoadRebalancePartitions;
  private SimpleDynamicMetric<Long> _numRecoveryRebalanceThrottledPartitions;
  private SimpleDynamicMetric<Long> _numLoadRebalanceThrottledPartitions;

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
  private ClusterStatusMonitor _clusterStatusMonitor;

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

  @Override
  public synchronized void unregister() {
    super.unregister();
    // Also remove metrics propagated to aggregate metrics in ClusterStatusMonitor
    if (_clusterStatusMonitor != null) {
      _clusterStatusMonitor.applyDeltaToTotalPartitionCount(-_numOfPartitions.getValue());
      _clusterStatusMonitor.applyDeltaToTotalErrorPartitionCount(-_numOfErrorPartitions.getValue());
      _clusterStatusMonitor.applyDeltaToTotalExternalViewIdealStateMismatchPartitionCount(
          -_externalViewIdealStateDiff.getValue());
      _clusterStatusMonitor.applyDeltaToTotalPartitionsWithoutTopStateCount(-_numNonTopStatePartitions.getValue());
    }
  }

  public enum MonitorState {
    TOP_STATE
  }

  public ResourceMonitor(String clusterName, String resourceName, ObjectName objectName) {
    this(null, clusterName, resourceName, objectName);
  }

  public ResourceMonitor(ClusterStatusMonitor clusterStatusMonitor, String clusterName, String resourceName,
      ObjectName objectName) {
    if (clusterStatusMonitor == null) {
      _logger.warn("ResourceMonitor initialized without a reference to ClusterStatusMonitor (null): metrics will not "
          + "be aggregated at the cluster level.");
    }
    _clusterStatusMonitor = clusterStatusMonitor;
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
    return String.format("%s.%s.%s.%s", ClusterStatusMonitor.RESOURCE_STATUS_KEY, _clusterName, _tag, _resourceName);
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
      _logger.warn("ideal state is null for {}", _resourceName);
      return;
    }

    assert (_resourceName.equals(idealState.getId()));
    assert (_resourceName.equals(externalView.getId()));

    long numOfErrorPartitions = 0;
    long numOfDiff = 0;
    long numOfPartitionWithTopState = 0;

    Set<String> partitions = idealState.getPartitionSet();
    int replica;
    try {
      replica = Integer.valueOf(idealState.getReplicas());
    } catch (NumberFormatException e) {
      _logger.info("Unspecified replica count for {}, skip updating the ResourceMonitor Mbean: {}", _resourceName,
          idealState.getReplicas());
      return;
    } catch (Exception ex) {
      _logger.warn("Failed to get replica count for {}, cannot update the ResourceMonitor Mbean.", _resourceName);
      return;
    }

    int minActiveReplica = idealState.getMinActiveReplicas();
    minActiveReplica = (minActiveReplica >= 0) ? minActiveReplica : replica;

    Set<String> activeStates = new HashSet<>(stateModelDef.getStatesPriorityList());
    activeStates.remove(stateModelDef.getInitialState());
    activeStates.remove(HelixDefinedState.DROPPED.name());
    activeStates.remove(HelixDefinedState.ERROR.name());

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

    // Update cluster-level aggregate metrics in ClusterStatusMonitor
    if (_clusterStatusMonitor != null) {
      _clusterStatusMonitor.applyDeltaToTotalPartitionCount(partitions.size() - _numOfPartitions.getValue());
      _clusterStatusMonitor.applyDeltaToTotalErrorPartitionCount(
          numOfErrorPartitions - _numOfErrorPartitions.getValue());
      _clusterStatusMonitor.applyDeltaToTotalExternalViewIdealStateMismatchPartitionCount(
          numOfDiff - _externalViewIdealStateDiff.getValue());
      _clusterStatusMonitor.applyDeltaToTotalPartitionsWithoutTopStateCount(
          (partitions.size() - numOfPartitionWithTopState) - _numNonTopStatePartitions.getValue());
    }

    // Update resource-level metrics
    _numOfPartitions.updateValue((long) partitions.size());
    _numOfErrorPartitions.updateValue(numOfErrorPartitions);
    _externalViewIdealStateDiff.updateValue(numOfDiff);
    _numOfPartitionsInExternalView.updateValue((long) externalView.getPartitionSet().size());
    _numNonTopStatePartitions.updateValue(_numOfPartitions.getValue() - numOfPartitionWithTopState);

    String tag = idealState.getInstanceGroupTag();
    if (tag == null || tag.equals("") || tag.equals("null")) {
      _tag = ClusterStatusMonitor.DEFAULT_TAG;
    } else {
      _tag = tag;
    }
  }

  private void resetGauges() {
    // Disable reset for the following gauges:
    // 1) Need the previous values for these gauges to compute delta for cluster-level metrics.
    // 2) These four gauges are reset every time updateResource is called anyway.
    //_numOfErrorPartitions.updateValue(0l);
    //_numNonTopStatePartitions.updateValue(0l);
    //_externalViewIdealStateDiff.updateValue(0l);
    //_numOfPartitionsInExternalView.updateValue(0l);

    // The following gauges are computed each call to updateResource by way of looping so need to be reset.
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

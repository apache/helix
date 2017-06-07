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

import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.*;
import org.apache.log4j.Logger;


public class ResourceMonitor implements ResourceMonitorMBean {
  private static final Logger LOG = Logger.getLogger(ResourceMonitor.class);
  private static final long RESET_TIME_RANGE = 1000 * 60 * 60; // 1 hour

  // Gauges
  private int _numOfPartitions;
  private int _numOfPartitionsInExternalView;
  private int _numOfErrorPartitions;
  private int _numNonTopStatePartitions;
  private long _numLessMinActiveReplicaPartitions;
  private long _numLessReplicaPartitions;
  private long _numPendingRecoveryRebalancePartitions;
  private long _numPendingLoadRebalancePartitions;
  private long _numRecoveryRebalanceThrottledPartitions;
  private long _numLoadRebalanceThrottledPartitions;
  private int _externalViewIdealStateDiff;

  // Counters
  private long _successfulTopStateHandoffDurationCounter;
  private long _successTopStateHandoffCounter;
  private long _failedTopStateHandoffCounter;
  private long _maxSinglePartitionTopStateHandoffDuration;

  private long _lastResetTime;
  private String _tag = ClusterStatusMonitor.DEFAULT_TAG;
  private String _resourceName;
  private String _clusterName;

  private long _totalMessageReceived;

  public enum MonitorState {
    TOP_STATE
  }

  public ResourceMonitor(String clusterName, String resourceName) {
    _clusterName = clusterName;
    _resourceName = resourceName;
    _successfulTopStateHandoffDurationCounter = 0L;
    _successTopStateHandoffCounter = 0L;
    _failedTopStateHandoffCounter = 0L;
    _lastResetTime = System.currentTimeMillis();
    _totalMessageReceived = 0L;
  }

  @Override
  public long getPartitionGauge() {
    return _numOfPartitions;
  }

  @Override
  public long getErrorPartitionGauge() {
    return _numOfErrorPartitions;
  }

  @Override
  public long getMissingTopStatePartitionGauge() {
    return _numNonTopStatePartitions;
  }

  @Override
  public long getDifferenceWithIdealStateGauge() {
    return _externalViewIdealStateDiff;
  }

  @Override
  public long getSuccessfulTopStateHandoffDurationCounter() {
    return _successfulTopStateHandoffDurationCounter;
  }

  @Override
  public long getSucceededTopStateHandoffCounter() {
    return _successTopStateHandoffCounter;
  }

  @Override
  public long getMaxSinglePartitionTopStateHandoffDurationGauge() {
    return _maxSinglePartitionTopStateHandoffDuration;
  }

  @Override
  public long getFailedTopStateHandoffCounter() {
    return _failedTopStateHandoffCounter;
  }

  @Override
  public long getTotalMessageReceived() {
    return _totalMessageReceived;
  }

  public synchronized void increaseMessageCount(long messageReceived) {
    _totalMessageReceived += messageReceived;
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s.%s", ClusterStatusMonitor.RESOURCE_STATUS_KEY, _clusterName,
        _tag, _resourceName);
  }

  public String getResourceName() {
    return _resourceName;
  }

  public void updateResource(ExternalView externalView, IdealState idealState, StateModelDefinition stateModelDef) {
    if (externalView == null) {
      LOG.warn("External view is null");
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
      LOG.warn("ideal state is null for " + _resourceName);
      return;
    }

    assert (_resourceName.equals(idealState.getId()));
    assert (_resourceName.equals(externalView.getId()));

    int numOfErrorPartitions = 0;
    int numOfDiff = 0;
    int numOfPartitionWithTopState = 0;

    Set<String> partitions = idealState.getPartitionSet();

    if (_numOfPartitions == 0) {
      _numOfPartitions = partitions.size();
    }

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

      if (externalViewRecord == null) {
        externalViewRecord = Collections.emptyMap();
      }
      if (!idealRecord.entrySet().equals(externalViewRecord.entrySet())) {
        numOfDiff++;
      }

      int activeReplicaCount = 0;
      for (String host : externalViewRecord.keySet()) {
        String currentState = externalViewRecord.get(host);
        if (HelixDefinedState.ERROR.toString().equalsIgnoreCase(currentState)) {
          numOfErrorPartitions++;
        }
        if (topState != null && topState.equalsIgnoreCase(currentState)) {
          numOfPartitionWithTopState++;
        }

        Map<String, Integer> stateCount = stateModelDef.getStateCountMap(idealRecord.size(), replica);
        Set<String> activeStates = stateCount.keySet();
        if (currentState != null && activeStates.contains(currentState)) {
          activeReplicaCount++;
        }
      }
      if (replica > 0 && activeReplicaCount < replica) {
        _numLessReplicaPartitions ++;
      }
      if (minActiveReplica >= 0 && activeReplicaCount < minActiveReplica) {
        _numLessMinActiveReplicaPartitions ++;
      }
    }
    _numOfErrorPartitions = numOfErrorPartitions;
    _externalViewIdealStateDiff = numOfDiff;
    _numOfPartitionsInExternalView = externalView.getPartitionSet().size();
    _numNonTopStatePartitions = _numOfPartitions - numOfPartitionWithTopState;

    String tag = idealState.getInstanceGroupTag();
    if (tag == null || tag.equals("") || tag.equals("null")) {
      _tag = ClusterStatusMonitor.DEFAULT_TAG;
    } else {
      _tag = tag;
    }
  }

  private void resetGauges() {
    _numOfErrorPartitions = 0;
    _numNonTopStatePartitions = 0;
    _externalViewIdealStateDiff = 0;
    _numOfPartitionsInExternalView = 0;
    _numLessMinActiveReplicaPartitions = 0;
    _numLessReplicaPartitions = 0;
    _numPendingRecoveryRebalancePartitions = 0;
    _numPendingLoadRebalancePartitions = 0;
    _numRecoveryRebalanceThrottledPartitions = 0;
    _numLoadRebalanceThrottledPartitions = 0;
  }

  public void updateStateHandoffStats(MonitorState monitorState, long duration, boolean succeeded) {
    switch (monitorState) {
    case TOP_STATE:
      if (succeeded) {
        _successTopStateHandoffCounter++;
        _successfulTopStateHandoffDurationCounter += duration;
        _maxSinglePartitionTopStateHandoffDuration =
            Math.max(_maxSinglePartitionTopStateHandoffDuration, duration);
      } else {
        _failedTopStateHandoffCounter++;
      }
      break;
    default:
      LOG.warn(
          String.format("Wrong monitor state \"%s\" that not supported ", monitorState.name()));
    }
  }

  public void updateRebalancerStat(long numPendingRecoveryRebalancePartitions,
      long numPendingLoadRebalancePartitions, long numRecoveryRebalanceThrottledPartitions,
      long numLoadRebalanceThrottledPartitions) {
    _numPendingRecoveryRebalancePartitions = numPendingRecoveryRebalancePartitions;
    _numPendingLoadRebalancePartitions = numPendingLoadRebalancePartitions;
    _numRecoveryRebalanceThrottledPartitions = numRecoveryRebalanceThrottledPartitions;
    _numLoadRebalanceThrottledPartitions = numLoadRebalanceThrottledPartitions;
  }

  @Override
  public long getExternalViewPartitionGauge() {
    return _numOfPartitionsInExternalView;
  }

  @Override
  public long getMissingMinActiveReplicaPartitionGauge() {
    return _numLessMinActiveReplicaPartitions;
  }

  @Override
  public long getMissingReplicaPartitionGauge() {
    return _numLessReplicaPartitions;
  }

  @Override
  public long getPendingRecoveryRebalancePartitionGauge() {
    return _numPendingRecoveryRebalancePartitions;
  }

  @Override
  public long getPendingLoadRebalancePartitionGauge() {
    return _numPendingLoadRebalancePartitions;
  }

  @Override
  public long getRecoveryRebalanceThrottledPartitionGauge() {
    return _numRecoveryRebalanceThrottledPartitions;
  }

  @Override
  public long getLoadRebalanceThrottledPartitionGauge() {
    return _numLoadRebalanceThrottledPartitions;
  }

  public String getBeanName() {
    return _clusterName + " " + _resourceName;
  }

  public void resetMaxTopStateHandoffGauge() {
    if (_lastResetTime + RESET_TIME_RANGE <= System.currentTimeMillis()) {
      _maxSinglePartitionTopStateHandoffDuration = 0L;
      _lastResetTime = System.currentTimeMillis();
    }
  }
}

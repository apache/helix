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

import org.apache.helix.task.TaskState;

public class WorkflowMonitor implements WorkflowMonitorMBean {
  private static final String WORKFLOW_KEY = "Workflow";
  private static final long DEFAULT_RESET_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

  private String _clusterName;
  private String _workflowType;

  private long _successfulWorkflowCount;
  private long _failedWorkflowCount;
  private long _failedWorkflowGauge;
  private long _existingWorkflowGauge;
  private long _queuedWorkflowGauge;
  private long _runningWorkflowGauge;
  private long _totalWorkflowLatencyCount;
  private long _maximumWorkflowLatencyGauge;
  private long _lastResetTime;


  public WorkflowMonitor(String clusterName, String workflowType) {
    _clusterName = clusterName;
    _workflowType = workflowType;
    _successfulWorkflowCount = 0L;
    _failedWorkflowCount = 0L;
    _failedWorkflowGauge = 0L;
    _existingWorkflowGauge = 0L;
    _queuedWorkflowGauge = 0L;
    _runningWorkflowGauge = 0L;
    _totalWorkflowLatencyCount = 0L;
    _maximumWorkflowLatencyGauge = 0L;
    _lastResetTime = System.currentTimeMillis();
  }

  @Override
  public long getSuccessfulWorkflowCount() {
    return _successfulWorkflowCount;
  }

  @Override
  public long getFailedWorkflowCount() {
    return _failedWorkflowCount;
  }

  @Override
  public long getFailedWorkflowGauge() {
    return _failedWorkflowGauge;
  }

  @Override
  public long getExistingWorkflowGauge() {
    return _existingWorkflowGauge;
  }

  @Override
  public long getQueuedWorkflowGauge() {
    return _queuedWorkflowGauge;
  }

  @Override
  public long getRunningWorkflowGauge() {
    return _runningWorkflowGauge;
  }

  @Override
  public long getWorkflowLatencyCount() {
    return _totalWorkflowLatencyCount;
  }

  @Override
  public long getMaximumWorkflowLatencyGauge() {
    return _maximumWorkflowLatencyGauge;
  }

  @Override public String getSensorName() {
    return String.format("%s.%s.%s", _clusterName, WORKFLOW_KEY, _workflowType);
  }

  public String getWorkflowType() {
    return _workflowType;
  }

  /**
   * Update workflow with transition state
   * @param to The to state of a workflow
   */

  public void updateWorkflowCounters(TaskState to) {
    updateWorkflowCounters(to, 0);
  }

  public void updateWorkflowCounters(TaskState to, long latency) {
    if (to.equals(TaskState.FAILED)) {
      _failedWorkflowCount++;
    } else if (to.equals(TaskState.COMPLETED)) {
      _successfulWorkflowCount++;

      // Only record latency larger than 0 and succeeded workflows
      _maximumWorkflowLatencyGauge = Math.max(_maximumWorkflowLatencyGauge, latency);
      _totalWorkflowLatencyCount += latency > 0 ? latency : 0;
    }
  }

  /**
   * Reset gauges
   */
  public void resetGauges() {
    _failedWorkflowGauge = 0L;
    _existingWorkflowGauge = 0L;
    _runningWorkflowGauge = 0L;
    _queuedWorkflowGauge = 0L;
    if (_lastResetTime + DEFAULT_RESET_INTERVAL_MS < System.currentTimeMillis()) {
      _lastResetTime = System.currentTimeMillis();
      _maximumWorkflowLatencyGauge = 0;
    }
  }

  /**
   * Refresh gauges via transition state
   * @param current current workflow state
   */
  public void updateWorkflowGauges(TaskState current) {
    if (current == null || current.equals(TaskState.NOT_STARTED)) {
      _queuedWorkflowGauge++;
    } else if (current.equals(TaskState.IN_PROGRESS)) {
      _runningWorkflowGauge++;
    } else if (current.equals(TaskState.FAILED)) {
      _failedWorkflowGauge++;
    }
    _existingWorkflowGauge++;
  }
}

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

import java.util.ArrayList;
import java.util.List;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.apache.helix.task.TaskState;

import static org.apache.helix.monitoring.mbeans.ClusterStatusMonitor.CLUSTER_DN_KEY;

public class WorkflowMonitor extends DynamicMBeanProvider {
  private static final String MBEAN_DESCRIPTION = "Workflow Monitor";
  private static final String WORKFLOW_KEY = "Workflow";
  static final String WORKFLOW_TYPE_DN_KEY = "workflowType";
  private static final long DEFAULT_RESET_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

  private String _clusterName;
  private String _workflowType;

  private SimpleDynamicMetric<Long> _successfulWorkflowCount;
  private SimpleDynamicMetric<Long> _failedWorkflowCount;
  private SimpleDynamicMetric<Long> _failedWorkflowGauge;
  private SimpleDynamicMetric<Long> _existingWorkflowGauge;
  private SimpleDynamicMetric<Long> _queuedWorkflowGauge;
  private SimpleDynamicMetric<Long> _runningWorkflowGauge;
  private SimpleDynamicMetric<Long> _totalWorkflowLatencyCount;
  private SimpleDynamicMetric<Long> _maximumWorkflowLatencyGauge;
  private SimpleDynamicMetric<Long> _lastResetTime;

  public WorkflowMonitor(String clusterName, String workflowType) {
    _clusterName = clusterName;
    _workflowType = workflowType;
    _successfulWorkflowCount = new SimpleDynamicMetric("SuccessfulWorkflowCount", 0L);
    _failedWorkflowCount = new SimpleDynamicMetric("FailedWorkflowCount", 0L);
    _failedWorkflowGauge = new SimpleDynamicMetric("FailedWorkflowGauge", 0L);
    _existingWorkflowGauge = new SimpleDynamicMetric("ExistingWorkflowGauge", 0L);
    _queuedWorkflowGauge = new SimpleDynamicMetric("QueuedWorkflowGauge", 0L);
    _runningWorkflowGauge = new SimpleDynamicMetric("RunningWorkflowGauge", 0L);
    _totalWorkflowLatencyCount = new SimpleDynamicMetric("TotalWorkflowLatencyCount", 0L);
    _maximumWorkflowLatencyGauge = new SimpleDynamicMetric("MaximumWorkflowLatencyGauge", 0L);
    _lastResetTime = new SimpleDynamicMetric("LastResetTime", System.currentTimeMillis());
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s", _clusterName, WORKFLOW_KEY, _workflowType);
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
      incrementSimpleDynamicMetric(_failedWorkflowCount, 1);
    } else if (to.equals(TaskState.COMPLETED)) {
      incrementSimpleDynamicMetric(_successfulWorkflowCount, 1);

      // Only record latency larger than 0 and succeeded workflows
      incrementSimpleDynamicMetric(_maximumWorkflowLatencyGauge,
          _maximumWorkflowLatencyGauge.getValue() > latency ? 0
              : latency - _maximumWorkflowLatencyGauge.getValue());
      incrementSimpleDynamicMetric(_totalWorkflowLatencyCount, latency > 0 ? latency : 0);
    }
  }

  /**
   * Reset gauges
   */
  public void resetGauges() {
    _failedWorkflowGauge.updateValue(0L);
    _existingWorkflowGauge.updateValue(0L);
    _runningWorkflowGauge.updateValue(0L);
    _queuedWorkflowGauge.updateValue(0L);
    if (_lastResetTime.getValue() + DEFAULT_RESET_INTERVAL_MS < System.currentTimeMillis()) {
      _lastResetTime.updateValue(System.currentTimeMillis());
      _maximumWorkflowLatencyGauge.updateValue(0L);
    }
  }

  /**
   * Refresh gauges via transition state
   * @param current current workflow state
   */
  public void updateWorkflowGauges(TaskState current) {
    if (current == null || current.equals(TaskState.NOT_STARTED)) {
      incrementSimpleDynamicMetric(_queuedWorkflowGauge);
    } else if (current.equals(TaskState.IN_PROGRESS)) {
      incrementSimpleDynamicMetric(_runningWorkflowGauge);
    } else if (current.equals(TaskState.FAILED)) {
      incrementSimpleDynamicMetric(_failedWorkflowGauge);
    }
    incrementSimpleDynamicMetric(_existingWorkflowGauge);
  }

  // All the get functions are for testing purpose only.
  public long getSuccessfulWorkflowCount() {
    return _successfulWorkflowCount.getValue();
  }

  public long getFailedWorkflowCount() {
    return _failedWorkflowCount.getValue();
  }

  public long getFailedWorkflowGauge() {
    return _failedWorkflowGauge.getValue();
  }

  public long getExistingWorkflowGauge() {
    return _existingWorkflowGauge.getValue();
  }

  public long getQueuedWorkflowGauge() {
    return _queuedWorkflowGauge.getValue();
  }

  public long getRunningWorkflowGauge() {
    return _runningWorkflowGauge.getValue();
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_successfulWorkflowCount);
    attributeList.add(_failedWorkflowCount);
    attributeList.add(_failedWorkflowGauge);
    attributeList.add(_existingWorkflowGauge);
    attributeList.add(_queuedWorkflowGauge);
    attributeList.add(_runningWorkflowGauge);
    attributeList.add(_totalWorkflowLatencyCount);
    attributeList.add(_maximumWorkflowLatencyGauge);
    attributeList.add(_lastResetTime);
    doRegister(attributeList, MBEAN_DESCRIPTION, getObjectName(_workflowType));
    return this;
  }

  private ObjectName getObjectName(String workflowType) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), String
        .format("%s, %s=%s", String.format("%s=%s", CLUSTER_DN_KEY, _clusterName),
            WORKFLOW_TYPE_DN_KEY, workflowType)));
  }
}

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

import java.util.Map;
import java.util.Set;

import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;

public class JobMonitor implements JobMonitorMBean {

  private static final String JOB_KEY = "Job";

  private String _clusterName;
  private Set<String> _workflows;
  private String _jobType;

  private long _allJobCount;
  private long _successfullJobCount;
  private long _failedJobCount;
  private long _existingJobGauge;
  private long _queuedJobGauge;
  private long _runningJobGauge;

  private TaskDriver _driver;

  public JobMonitor(String clusterName, String jobType, TaskDriver driver) {
    _clusterName = clusterName;
    _workflows = driver.getWorkflows().keySet();
    _jobType = jobType;
    _allJobCount = 0L;
    _successfullJobCount = 0L;
    _failedJobCount = 0L;
    _existingJobGauge = 0L;
    _queuedJobGauge = 0L;
    _runningJobGauge = 0L;
    _driver = driver;
  }

  @Override
  public long getAllJobCount() {
    return _allJobCount;
  }

  @Override
  public long getSuccessfulJobCount() {
    return _successfullJobCount;
  }

  @Override
  public long getFailedJobCount() {
    return _failedJobCount;
  }

  @Override
  public long getExistingJobGauge() {
    return _existingJobGauge;
  }

  @Override
  public long getQueuedJobGauge() {
    return _queuedJobGauge;
  }

  @Override
  public long getRunningJobGauge() {
    return _runningJobGauge;
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s", _clusterName, JOB_KEY, _jobType);
  }

  /**
   * Update job metrics with transition state
   * @param from The from state of job, just created when it is null
   * @param to The to state of job, cleaned by ZK when it is null
   */
  public void updateJobStats(TaskState from, TaskState to) {
    if (from == null) {
      // From null means a new job has been created
      _existingJobGauge++;
      _queuedJobGauge++;
      _allJobCount++;
    } else if (from.equals(TaskState.NOT_STARTED)) {
      // From NOT_STARTED means queued job number has been decreased one
      _queuedJobGauge--;
    } else if (from.equals(TaskState.IN_PROGRESS)) {
      // From IN_PROGRESS means running job number has been decreased one
      _runningJobGauge--;
    }

    if (to == null) {
      // To null means the job has been cleaned from ZK
      _existingJobGauge--;
    } else if (to.equals(TaskState.IN_PROGRESS)) {
      _runningJobGauge++;
    } else if (to.equals(TaskState.FAILED)) {
      _failedJobCount++;
    } else if (to.equals(TaskState.COMPLETED)) {
      _successfullJobCount++;
    }
  }

  /**
   * Refresh job metrics when instance reconnected
   */
  public void refreshJobStats() {
    _allJobCount = 0L;
    _successfullJobCount = 0L;
    _failedJobCount = 0L;
    _queuedJobGauge = 0L;
    _runningJobGauge = 0L;
    _workflows = _driver.getWorkflows().keySet();

    for (String workflow : _workflows) {
      Set<String> allJobs = _driver.getWorkflowConfig(workflow).getJobDag().getAllNodes();
      WorkflowContext workflowContext = _driver.getWorkflowContext(workflow);

      for (String job : allJobs) {
        JobContext jobContext = _driver.getJobContext(job);
        if (jobContext == null) {
          _queuedJobGauge++;
        } else if (workflowContext != null && workflowContext.getJobState(job)
            .equals(TaskState.IN_PROGRESS)) {
          _runningJobGauge++;
        }
      }

      _existingJobGauge += allJobs.size();
    }
  }
}

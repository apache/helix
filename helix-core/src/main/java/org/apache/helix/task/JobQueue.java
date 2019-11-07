package org.apache.helix.task;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;

/**
 * A named queue to which jobs can be added
 */
public class JobQueue extends Workflow {

  private JobQueue(String name, WorkflowConfig workflowConfig,
      Map<String, Map<String, String>> jobConfigs, Map<String, List<TaskConfig>> taskConfigs) {
    super(name, workflowConfig, jobConfigs, taskConfigs);
  }

  /**
   * Determine the number of jobs that this queue can accept before rejecting further jobs
   * This method is deprecated, please use:
   * JobQueue.getWorkflowConfig().getCapacity();
   * @return queue capacity
   */
  @Deprecated
  public int getCapacity() {
    return _workflowConfig.getCapacity();
  }

  /** Supports creation of a single queue */
  public static class Builder extends Workflow.Builder {
    private List<String> jobs;

    public Builder(String name) {
      super(name);
      jobs = new ArrayList<String>();
    }

    /**
     * Do not use this method, use workflowConfigBuilder.setCapacity() instead.
     * If you set capacity via this method, the number given here
     * will override capacity number set from other places.
     * @param capacity
     * @return
     */
    @Override
    public Builder setCapacity(int capacity) {
      super.setCapacity(capacity);
      return this;
    }

    public Builder enqueueJob(final String job, JobConfig.Builder jobBuilder) {
      if (_workflowConfigBuilder != null) {
        if (jobs.size() >= _workflowConfigBuilder.getCapacity()) {
          throw new HelixException("Failed to push new job to jobQueue, it is already full");
        }
      }

      addJob(job, jobBuilder);
      if (jobs.size() > 0) {
        String previousJob = jobs.get(jobs.size() - 1);
        addParentChildDependency(previousJob, job);
      }
      jobs.add(job);
      return this;
    }

    /**
     * Please use setWorkflowConfigMap() instead.
     * @param workflowCfgMap
     * @return
     */
    @Override
    public Builder fromMap(Map<String, String> workflowCfgMap) {
      return setWorkflowConfigMap(workflowCfgMap);
    }

    @Override
    public Builder setWorkflowConfigMap(Map<String, String> workflowCfgMap) {
      super.setWorkflowConfigMap(workflowCfgMap);
      return this;
    }

    @Override
    public Builder setWorkflowConfig(WorkflowConfig workflowConfig) {
      super.setWorkflowConfig(workflowConfig);
      return this;
    }

    @Override
    public Builder setScheduleConfig(ScheduleConfig scheduleConfig) {
      super.setScheduleConfig(scheduleConfig);
      return this;
    }

    @Override
    public Builder setExpiry(long expiry) {
      super.setExpiry(expiry);
      return this;
    }

    @Override
    public JobQueue build() {
      buildConfig();
      _workflowConfigBuilder.setTerminable(false);
      _workflowConfigBuilder.setJobQueue(true);
      return new JobQueue(_name, _workflowConfigBuilder.build(), _jobConfigs, _taskConfigs);
    }
  }
}

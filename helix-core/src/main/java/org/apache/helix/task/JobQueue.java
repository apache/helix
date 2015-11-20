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

import org.apache.helix.HelixException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A named queue to which jobs can be added
 */
public class JobQueue extends Workflow {
  /* Config fields */
  public static final String CAPACITY = "CAPACITY";

  private final int _capacity;

  private JobQueue(String name, int capacity, WorkflowConfig workflowConfig,
      Map<String, Map<String, String>> jobConfigs, Map<String, List<TaskConfig>> taskConfigs) {
    super(name, workflowConfig, jobConfigs, taskConfigs);
    _capacity = capacity;
    validate();
  }

  /**
   * Determine the number of jobs that this queue can accept before rejecting further jobs
   * @return queue capacity
   */
  public int getCapacity() {
    return _capacity;
  }

  public Map<String, String> getResourceConfigMap() throws Exception {
    Map<String, String> cfgMap = _workflowConfig.getResourceConfigMap();
    cfgMap.put(CAPACITY, String.valueOf(_capacity));
    return cfgMap;
  }

  /** Supports creation of a single queue */
  public static class Builder extends Workflow.Builder {
    private int _capacity = Integer.MAX_VALUE;
    private List<String> jobs;

    public Builder(String name) {
      super(name);
      jobs = new ArrayList<String>();
    }

    public Builder expiry(long expiry) {
      _expiry = expiry;
      return this;
    }

    public Builder capacity(int capacity) {
      _capacity = capacity;
      return this;
    }

    @Override
    public Builder fromMap(Map<String, String> cfg) {
      super.fromMap(cfg);
      if (cfg.containsKey(CAPACITY)) {
        _capacity = Integer.parseInt(cfg.get(CAPACITY));
      }
      return this;
    }

    public void enqueueJob(final String job, JobConfig.Builder jobBuilder) {
      if (jobs.size() >= _capacity) {
        throw new HelixException("Failed to push new job to jobQueue, it is already full");
      }
      addJobConfig(job, jobBuilder);
      if (jobs.size() > 0) {
        String previousJob = jobs.get(jobs.size() - 1);
        addParentChildDependency(previousJob, job);
      }
      jobs.add(job);
    }

    public JobQueue build() {
      WorkflowConfig.Builder builder = buildWorkflowConfig();
      builder.setTerminable(false);
      WorkflowConfig workflowConfig = builder.build();
      return new JobQueue(_name, _capacity, workflowConfig, _jobConfigs, _taskConfigs);
    }
  }
}

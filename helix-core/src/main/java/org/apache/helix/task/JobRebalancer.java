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

import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom rebalancer implementation for the {@code Job} in task model.
 */
public class JobRebalancer extends TaskRebalancer {
  private static final Logger LOG = LoggerFactory.getLogger(JobRebalancer.class);
  private JobDispatcher _jobDispatcher;

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(
      WorkflowControllerDataProvider clusterData, IdealState taskIs, Resource resource,
      CurrentStateOutput currStateOutput) {
    long startTime = System.currentTimeMillis();
    final String jobName = resource.getResourceName();
    JobConfig jobConfig = clusterData.getJobConfig(jobName);
    if (jobConfig == null) {
      LOG.error(
          "Job {}'s JobConfig is missing. This job might have been deleted or purged. Skipping status update and assignment!",
          jobName);
      return buildEmptyAssignment(jobName, currStateOutput);
    }
    LOG.debug("Computer Best Partition for job: " + jobName);
    if (_jobDispatcher == null) {
      _jobDispatcher = new JobDispatcher();
    }
    _jobDispatcher.init(_manager);
    _jobDispatcher.updateCache(clusterData);
    _jobDispatcher.setClusterStatusMonitor(_clusterStatusMonitor);
    ResourceAssignment resourceAssignment = _jobDispatcher.processJobStatusUpdateAndAssignment(
        jobName, currStateOutput, clusterData.getWorkflowContext(jobConfig.getWorkflow()));
    LOG.debug(String.format("JobRebalancer computation takes %d ms for Job %s",
        System.currentTimeMillis() - startTime, jobName));
    return resourceAssignment;
  }
}

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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.task.assigner.ThreadCountBasedTaskAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

/**
 * Custom rebalancer implementation for the {@code Job} in task model.
 */
public class JobRebalancer extends TaskRebalancer {
  private static final Logger LOG = LoggerFactory.getLogger(JobRebalancer.class);
  private JobDispatcher _jobDispatcher;

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache clusterData,
      IdealState taskIs, Resource resource, CurrentStateOutput currStateOutput) {
    long startTime = System.currentTimeMillis();
    final String jobName = resource.getResourceName();
    LOG.debug("Computer Best Partition for job: " + jobName);

    if (_jobDispatcher == null) {
      _jobDispatcher = new JobDispatcher();
    }
    _jobDispatcher.init(_manager);
    _jobDispatcher.updateCache(clusterData);
    _jobDispatcher.setClusterStatusMonitor(_clusterStatusMonitor);
    ResourceAssignment resourceAssignment = _jobDispatcher
        .processJobStatusUpdateandAssignment(jobName, currStateOutput,
            clusterData.getWorkflowContext(clusterData.getJobConfig(jobName).getWorkflow()));
    LOG.debug(String.format("JobRebalancer computation takes %d ms for Job %s",
        System.currentTimeMillis() - startTime, jobName));
    return resourceAssignment;
  }
}

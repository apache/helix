package org.apache.helix.task.beans;

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

import java.util.List;
import java.util.Map;

import org.apache.helix.task.JobConfig;

/**
 * Bean class used for parsing job definitions from YAML.
 */
public class JobBean {
  public String name;
  public List<String> parents;
  public String targetResource;
  public List<String> targetPartitionStates;
  public List<String> targetPartitions;
  public String command;
  public Map<String, String> jobConfigMap;
  public List<TaskBean> tasks;
  public long timeoutPerPartition = JobConfig.DEFAULT_TIMEOUT_PER_TASK;
  public int numConcurrentTasksPerInstance = JobConfig.DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
  public int maxAttemptsPerTask = JobConfig.DEFAULT_MAX_ATTEMPTS_PER_TASK;
  public int maxForcedReassignmentsPerTask = JobConfig.DEFAULT_MAX_FORCED_REASSIGNMENTS_PER_TASK;
  public int failureThreshold = JobConfig.DEFAULT_FAILURE_THRESHOLD;
  public long taskRetryDelay = JobConfig.DEFAULT_TASK_RETRY_DELAY;
}

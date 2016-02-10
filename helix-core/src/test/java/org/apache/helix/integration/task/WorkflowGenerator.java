package org.apache.helix.integration.task;

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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Workflow;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Convenience class for generating various test workflows
 */
public class WorkflowGenerator {
  private static final Logger LOG = Logger.getLogger(WorkflowGenerator.class);

  public static final String DEFAULT_TGT_DB = "TestDB";
  public static final String JOB_NAME_1 = "SomeJob1";
  public static final String JOB_NAME_2 = "SomeJob2";

  public static final Map<String, String> DEFAULT_JOB_CONFIG;
  static {
    Map<String, String> tmpMap = new TreeMap<String, String>();
    tmpMap.put("TargetResource", DEFAULT_TGT_DB);
    tmpMap.put("TargetPartitionStates", "MASTER");
    tmpMap.put("Command", "Reindex");
    tmpMap.put("TimeoutPerPartition", String.valueOf(10 * 1000));
    DEFAULT_JOB_CONFIG = Collections.unmodifiableMap(tmpMap);
  }

  public static final Map<String, String> DEFAULT_COMMAND_CONFIG;
  static {
    Map<String, String> tmpMap = new TreeMap<String, String>();
    tmpMap.put("Timeout", String.valueOf(2000));
    DEFAULT_COMMAND_CONFIG = Collections.unmodifiableMap(tmpMap);
  }

  private static final JobConfig.Builder DEFAULT_JOB_BUILDER;
  static {
    JobConfig.Builder builder = JobConfig.Builder.fromMap(DEFAULT_JOB_CONFIG);
    builder.setJobCommandConfigMap(DEFAULT_COMMAND_CONFIG);
    DEFAULT_JOB_BUILDER = builder;
  }

  public static Workflow.Builder generateDefaultSingleJobWorkflowBuilder(String jobName) {
    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(DEFAULT_COMMAND_CONFIG);
    return generateSingleJobWorkflowBuilder(jobName, jobBuilder);
  }

  public static Workflow.Builder generateSingleJobWorkflowBuilder(String jobName,
      JobConfig.Builder jobBuilder) {
    return new Workflow.Builder(jobName).addJobConfig(jobName, jobBuilder);
  }

  public static Workflow.Builder generateDefaultRepeatedJobWorkflowBuilder(String workflowName) {
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    builder.addParentChildDependency(JOB_NAME_1, JOB_NAME_2);

    JobConfig.Builder jobBuilder = JobConfig.Builder.fromMap(DEFAULT_JOB_CONFIG);
    jobBuilder.setJobCommandConfigMap(DEFAULT_COMMAND_CONFIG);

    builder.addJobConfig(JOB_NAME_1, jobBuilder);
    builder.addJobConfig(JOB_NAME_2, jobBuilder);

    return builder;
  }
}

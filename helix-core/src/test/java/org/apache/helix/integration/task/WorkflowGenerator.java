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

  public static Workflow.Builder generateDefaultSingleJobWorkflowBuilderWithExtraConfigs(
      String jobName, Map<String, String> commandConfig, String... cfgs) {
    if (cfgs.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Additional configs should have even number of keys and values");
    }
    Workflow.Builder bldr = generateDefaultSingleJobWorkflowBuilder(jobName);
    for (int i = 0; i < cfgs.length; i += 2) {
      bldr.addConfig(jobName, cfgs[i], cfgs[i + 1]);
    }

    return bldr;
  }

  public static Workflow.Builder generateDefaultSingleJobWorkflowBuilder(String jobName) {
    return generateSingleJobWorkflowBuilder(jobName, DEFAULT_COMMAND_CONFIG, DEFAULT_JOB_CONFIG);
  }

  public static Workflow.Builder generateSingleJobWorkflowBuilder(String jobName,
      Map<String, String> commandConfig, Map<String, String> config) {
    Workflow.Builder builder = new Workflow.Builder(jobName);
    for (String key : config.keySet()) {
      builder.addConfig(jobName, key, config.get(key));
    }
    if (commandConfig != null) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        String serializedMap = mapper.writeValueAsString(commandConfig);
        builder.addConfig(jobName, JobConfig.JOB_COMMAND_CONFIG_MAP, serializedMap);
      } catch (IOException e) {
        LOG.error("Error serializing " + commandConfig, e);
      }
    }
    return builder;
  }

  public static Workflow.Builder generateDefaultRepeatedJobWorkflowBuilder(String workflowName) {
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    builder.addParentChildDependency(JOB_NAME_1, JOB_NAME_2);

    for (String key : DEFAULT_JOB_CONFIG.keySet()) {
      builder.addConfig(JOB_NAME_1, key, DEFAULT_JOB_CONFIG.get(key));
      builder.addConfig(JOB_NAME_2, key, DEFAULT_JOB_CONFIG.get(key));
    }
    ObjectMapper mapper = new ObjectMapper();
    try {
      String serializedMap = mapper.writeValueAsString(DEFAULT_COMMAND_CONFIG);
      builder.addConfig(JOB_NAME_1, JobConfig.JOB_COMMAND_CONFIG_MAP, serializedMap);
      builder.addConfig(JOB_NAME_2, JobConfig.JOB_COMMAND_CONFIG_MAP, serializedMap);
    } catch (IOException e) {
      LOG.error("Error serializing " + DEFAULT_COMMAND_CONFIG, e);
    }
    return builder;
  }
}

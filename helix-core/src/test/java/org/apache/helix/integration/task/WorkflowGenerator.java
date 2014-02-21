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

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.task.Workflow;

/**
 * Convenience class for generating various test workflows
 */
public class WorkflowGenerator {
  public static final String DEFAULT_TGT_DB = "TestDB";
  private static final String TASK_NAME_1 = "SomeTask1";
  private static final String TASK_NAME_2 = "SomeTask2";

  private static final Map<String, String> DEFAULT_TASK_CONFIG;
  static {
    Map<String, String> tmpMap = new TreeMap<String, String>();
    tmpMap.put("TargetResource", DEFAULT_TGT_DB);
    tmpMap.put("TargetPartitionStates", "MASTER");
    tmpMap.put("Command", "Reindex");
    tmpMap.put("CommandConfig", String.valueOf(2000));
    tmpMap.put("TimeoutPerPartition", String.valueOf(10 * 1000));
    DEFAULT_TASK_CONFIG = Collections.unmodifiableMap(tmpMap);
  }

  public static Workflow.Builder generateDefaultSingleTaskWorkflowBuilderWithExtraConfigs(
      String taskName, String... cfgs) {
    if (cfgs.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Additional configs should have even number of keys and values");
    }
    Workflow.Builder bldr = generateDefaultSingleTaskWorkflowBuilder(taskName);
    for (int i = 0; i < cfgs.length; i += 2) {
      bldr.addConfig(taskName, cfgs[i], cfgs[i + 1]);
    }

    return bldr;
  }

  public static Workflow.Builder generateDefaultSingleTaskWorkflowBuilder(String taskName) {
    return generateSingleTaskWorkflowBuilder(taskName, DEFAULT_TASK_CONFIG);
  }

  public static Workflow.Builder generateSingleTaskWorkflowBuilder(String taskName,
      Map<String, String> config) {
    Workflow.Builder builder = new Workflow.Builder(taskName);
    for (String key : config.keySet()) {
      builder.addConfig(taskName, key, config.get(key));
    }
    return builder;
  }

  public static Workflow.Builder generateDefaultRepeatedTaskWorkflowBuilder(String workflowName) {
    return generateRepeatedTaskWorkflowBuilder(workflowName, DEFAULT_TASK_CONFIG);
  }

  public static Workflow.Builder generateRepeatedTaskWorkflowBuilder(String workflowName,
      Map<String, String> config) {
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    builder.addParentChildDependency(TASK_NAME_1, TASK_NAME_2);

    for (String key : config.keySet()) {
      builder.addConfig(TASK_NAME_1, key, config.get(key));
      builder.addConfig(TASK_NAME_2, key, config.get(key));
    }

    return builder;
  }
}

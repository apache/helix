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

import com.google.common.base.Joiner;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.task.beans.TaskBean;
import org.apache.helix.task.beans.WorkflowBean;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

/**
 * Houses a task dag and config set to fully describe a task workflow
 */
public class Workflow {
  /** Default workflow name, useful constant for single-node workflows */
  public static enum WorkflowEnum {
    UNSPECIFIED;
  }

  /** Workflow name */
  private final String _name;

  /** Holds workflow-level configurations */
  private final WorkflowConfig _workflowConfig;

  /** Contains the per-task configurations for all tasks specified in the provided dag */
  private final Map<String, Map<String, String>> _taskConfigs;

  /** Constructs and validates a workflow against a provided dag and config set */
  private Workflow(String name, WorkflowConfig workflowConfig,
      Map<String, Map<String, String>> taskConfigs) {
    _name = name;
    _workflowConfig = workflowConfig;
    _taskConfigs = taskConfigs;

    validate();
  }

  public String getName() {
    return _name;
  }

  public Map<String, Map<String, String>> getTaskConfigs() {
    return _taskConfigs;
  }

  public Map<String, String> getResourceConfigMap() throws Exception {
    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put(WorkflowConfig.DAG, _workflowConfig.getTaskDag().toJson());
    cfgMap.put(WorkflowConfig.EXPIRY, String.valueOf(_workflowConfig.getExpiry()));
    cfgMap.put(WorkflowConfig.TARGET_STATE, _workflowConfig.getTargetState().name());

    return cfgMap;
  }

  /**
   * Parses the YAML description from a file into a {@link Workflow} object.
   * @param file An abstract path name to the file containing the workflow description.
   * @return A {@link Workflow} object.
   * @throws Exception
   */
  public static Workflow parse(File file) throws Exception {
    BufferedReader br = new BufferedReader(new FileReader(file));
    return parse(br);
  }

  /**
   * Parses a YAML description of the workflow into a {@link Workflow} object. The YAML string is of
   * the following
   * form:
   * <p/>
   *
   * <pre>
   * name: MyFlow
   * tasks:
   *   - name : TaskA
   *     command : SomeTask
   *     ...
   *   - name : TaskB
   *     parents : [TaskA]
   *     command : SomeOtherTask
   *     ...
   *   - name : TaskC
   *     command : AnotherTask
   *     ...
   *   - name : TaskD
   *     parents : [TaskB, TaskC]
   *     command : AnotherTask
   *     ...
   * </pre>
   * @param yaml A YAML string of the above form
   * @return A {@link Workflow} object.
   */
  public static Workflow parse(String yaml) throws Exception {
    return parse(new StringReader(yaml));
  }

  /** Helper function to parse workflow from a generic {@link Reader} */
  private static Workflow parse(Reader reader) throws Exception {
    Yaml yaml = new Yaml(new Constructor(WorkflowBean.class));
    WorkflowBean wf = (WorkflowBean) yaml.load(reader);
    Builder builder = new Builder(wf.name);

    for (TaskBean task : wf.tasks) {
      if (task.name == null) {
        throw new IllegalArgumentException("A task must have a name.");
      }

      if (task.parents != null) {
        for (String parent : task.parents) {
          builder.addParentChildDependency(parent, task.name);
        }
      }

      builder.addConfig(task.name, TaskConfig.WORKFLOW_ID, wf.name);
      builder.addConfig(task.name, TaskConfig.COMMAND, task.command);
      if (task.commandConfig != null) {
        builder.addConfig(task.name, TaskConfig.COMMAND_CONFIG, task.commandConfig.toString());
      }
      builder.addConfig(task.name, TaskConfig.TARGET_RESOURCE, task.targetResource);
      if (task.targetPartitionStates != null) {
        builder.addConfig(task.name, TaskConfig.TARGET_PARTITION_STATES,
            Joiner.on(",").join(task.targetPartitionStates));
      }
      if (task.targetPartitions != null) {
        builder.addConfig(task.name, TaskConfig.TARGET_PARTITIONS,
            Joiner.on(",").join(task.targetPartitions));
      }
      builder.addConfig(task.name, TaskConfig.MAX_ATTEMPTS_PER_PARTITION,
          String.valueOf(task.maxAttemptsPerPartition));
      builder.addConfig(task.name, TaskConfig.NUM_CONCURRENT_TASKS_PER_INSTANCE,
          String.valueOf(task.numConcurrentTasksPerInstance));
      builder.addConfig(task.name, TaskConfig.TIMEOUT_PER_PARTITION,
          String.valueOf(task.timeoutPerPartition));
    }

    return builder.build();
  }

  /**
   * Verifies that all nodes in provided dag have accompanying config and vice-versa.
   * Also checks dag for cycles and unreachable nodes, and ensures configs are valid.
   */
  public void validate() {
    // validate dag and configs
    if (!_taskConfigs.keySet().containsAll(_workflowConfig.getTaskDag().getAllNodes())) {
      throw new IllegalArgumentException("Nodes specified in DAG missing from config");
    } else if (!_workflowConfig.getTaskDag().getAllNodes().containsAll(_taskConfigs.keySet())) {
      throw new IllegalArgumentException("Given DAG lacks nodes with supplied configs");
    }

    _workflowConfig.getTaskDag().validate();

    for (String node : _taskConfigs.keySet()) {
      buildConfig(node);
    }
  }

  /** Builds a TaskConfig from config map. Useful for validating configs */
  private TaskConfig buildConfig(String task) {
    return TaskConfig.Builder.fromMap(_taskConfigs.get(task)).build();
  }

  /** Build a workflow incrementally from dependencies and single configs, validate at build time */
  public static class Builder {
    private final String _name;
    private final TaskDag _dag;
    private final Map<String, Map<String, String>> _taskConfigs;
    private long _expiry;

    public Builder(String name) {
      _name = name;
      _dag = new TaskDag();
      _taskConfigs = new TreeMap<String, Map<String, String>>();
      _expiry = -1;
    }

    public Builder addConfig(String node, String key, String val) {
      node = namespacify(node);
      _dag.addNode(node);

      if (!_taskConfigs.containsKey(node)) {
        _taskConfigs.put(node, new TreeMap<String, String>());
      }
      _taskConfigs.get(node).put(key, val);

      return this;
    }

    public Builder addParentChildDependency(String parent, String child) {
      parent = namespacify(parent);
      child = namespacify(child);
      _dag.addParentToChild(parent, child);

      return this;
    }

    public Builder setExpiry(long expiry) {
      _expiry = expiry;
      return this;
    }

    public String namespacify(String task) {
      return TaskUtil.getNamespacedTaskName(_name, task);
    }

    public Workflow build() {
      for (String task : _taskConfigs.keySet()) {
        // addConfig(task, TaskConfig.WORKFLOW_ID, _name);
        _taskConfigs.get(task).put(TaskConfig.WORKFLOW_ID, _name);
      }

      WorkflowConfig.Builder builder = new WorkflowConfig.Builder();
      builder.setTaskDag(_dag);
      builder.setTargetState(TargetState.START);
      if (_expiry > 0) {
        builder.setExpiry(_expiry);
      }

      return new Workflow(_name, builder.build(), _taskConfigs); // calls validate internally
    }
  }
}

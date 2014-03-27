package org.apache.helix.integration.task;

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
    Workflow.Builder builder = new Workflow.Builder(workflowName);
    builder.addParentChildDependency(TASK_NAME_1, TASK_NAME_2);

    for (String key : DEFAULT_TASK_CONFIG.keySet()) {
      builder.addConfig(TASK_NAME_1, key, DEFAULT_TASK_CONFIG.get(key));
      builder.addConfig(TASK_NAME_2, key, DEFAULT_TASK_CONFIG.get(key));
    }

    return builder;
  }
}

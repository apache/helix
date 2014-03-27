/*
 * $Id$
 */
package org.apache.helix.task.beans;

import java.util.List;
import java.util.Map;

import org.apache.helix.task.TaskConfig;

/**
 * Bean class used for parsing task definitions from YAML.
 */
public class TaskBean {
  public String name;
  public List<String> parents;
  public String targetResource;
  public List<String> targetPartitionStates;
  public List<Integer> targetPartitions;
  public String command;
  public Map<String, Object> commandConfig;
  public long timeoutPerPartition = TaskConfig.DEFAULT_TIMEOUT_PER_PARTITION;
  public int numConcurrentTasksPerInstance = TaskConfig.DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
  public int maxAttemptsPerPartition = TaskConfig.DEFAULT_MAX_ATTEMPTS_PER_PARTITION;
}

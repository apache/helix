package org.apache.helix.api;

import java.util.Map;

import org.apache.helix.model.Message;

import com.google.common.collect.ImmutableMap;

public class SchedulerTaskConfig {
  // TODO refactor using Transition logical model
  private final Map<String, Integer> _transitionTimeoutMap;

  // TODO refactor this when understand inner message format
  private final Map<PartitionId, Map<String, String>> _schedulerTaskConfig;

  public SchedulerTaskConfig(Map<String, Integer> transitionTimeoutMap,
      Map<PartitionId, Map<String, String>> schedulerTaskConfig) {
    _transitionTimeoutMap = ImmutableMap.copyOf(transitionTimeoutMap);
    _schedulerTaskConfig = ImmutableMap.copyOf(schedulerTaskConfig);
  }

  public Map<String, String> getTaskConfig(PartitionId partitionId) {
    return _schedulerTaskConfig.get(partitionId);
  }

  public Integer getTransitionTimeout(String transition) {
    return _transitionTimeoutMap.get(transition);
  }

  public Integer getTimeout(String transition, PartitionId partitionId) {
    Integer timeout = getTransitionTimeout(transition);
    if (timeout == null) {
      Map<String, String> taskConfig = getTaskConfig(partitionId);
      if (taskConfig != null) {
        String timeoutStr = taskConfig.get(Message.Attributes.TIMEOUT.toString());
        if (timeoutStr != null) {
          try {
            timeout = Integer.parseInt(timeoutStr);
          } catch (Exception e) {
            // ignore
          }
        }
      }
    }
    return timeout;
  }
}

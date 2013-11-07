package org.apache.helix.api.config;

import java.util.Map;
import java.util.Set;

import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;

import com.google.common.collect.ImmutableMap;

public class SchedulerTaskConfig {
  // TODO refactor using Transition logical model
  private final Map<String, Integer> _transitionTimeoutMap;

  private final Map<PartitionId, Message> _innerMessageMap;

  public SchedulerTaskConfig(Map<String, Integer> transitionTimeoutMap,
      Map<PartitionId, Message> innerMsgMap) {
    _transitionTimeoutMap = ImmutableMap.copyOf(transitionTimeoutMap);
    _innerMessageMap = ImmutableMap.copyOf(innerMsgMap);
  }

  /**
   * Get inner message for a partition
   * @param partitionId
   * @return inner message
   */
  public Message getInnerMessage(PartitionId partitionId) {
    return _innerMessageMap.get(partitionId);
  }

  /**
   * Get timeout for a transition
   * @param transition
   * @return timeout or -1 if not available
   */
  public int getTransitionTimeout(String transition) {
    Integer timeout = _transitionTimeoutMap.get(transition);
    if (timeout == null) {
      return -1;
    }

    return timeout;
  }

  /**
   * Get timeout for an inner message
   * @param transition
   * @param partitionId
   * @return timeout or -1 if not available
   */
  public int getTimeout(String transition, PartitionId partitionId) {
    Integer timeout = getTransitionTimeout(transition);
    if (timeout == null) {
      Message innerMessage = getInnerMessage(partitionId);
      timeout = innerMessage.getTimeout();
    }

    return timeout;
  }

  /**
   * Get partition-id set
   * @return partition-id set
   */
  public Set<PartitionId> getPartitionSet() {
    return _innerMessageMap.keySet();
  }
}

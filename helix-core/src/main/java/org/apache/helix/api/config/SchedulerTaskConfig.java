package org.apache.helix.api.config;

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

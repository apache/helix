package org.apache.helix.api;

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

import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;

import com.google.common.collect.ImmutableMap;

/**
 * A cluster participant
 */
public class Participant {
  private final ParticipantConfig _config;

  private final RunningInstance _runningInstance;

  /**
   * map of resource-id to current-state
   */
  private final Map<ResourceId, CurrentState> _currentStateMap;

  /**
   * map of message-id to message
   */
  private final Map<MessageId, Message> _messageMap;

  /**
   * Construct a participant
   * @param config
   */
  public Participant(ParticipantId id, String hostName, int port, boolean isEnabled,
      Set<PartitionId> disabledPartitionIdSet, Set<String> tags, RunningInstance runningInstance,
      Map<ResourceId, CurrentState> currentStateMap, Map<MessageId, Message> messageMap,
      UserConfig userConfig) {
    _config =
        new ParticipantConfig(id, hostName, port, isEnabled, disabledPartitionIdSet, tags,
            userConfig);
    _runningInstance = runningInstance;
    _currentStateMap = ImmutableMap.copyOf(currentStateMap);
    _messageMap = ImmutableMap.copyOf(messageMap);
  }

  /**
   * Get the host name of the participant
   * @return host name, or null if not applicable
   */
  public String getHostName() {
    return _config.getHostName();
  }

  /**
   * Get the port of the participant
   * @return port number, or -1 if not applicable
   */
  public int getPort() {
    return _config.getPort();
  }

  /**
   * Get if the participant is enabled
   * @return true if enabled or false otherwise
   */
  public boolean isEnabled() {
    return _config.isEnabled();
  }

  /**
   * Get if the participant is alive
   * @return true if running or false otherwise
   */
  public boolean isAlive() {
    return _runningInstance != null;
  }

  /**
   * Get the running instance
   * @return running instance or null if not running
   */
  public RunningInstance getRunningInstance() {
    return _runningInstance;
  }

  /**
   * Get disabled partition id's
   * @return set of disabled partition id's, or empty set if none
   */
  public Set<PartitionId> getDisablePartitionIds() {
    return _config.getDisabledPartitions();
  }

  /**
   * Get tags
   * @return set of tags
   */
  public Set<String> getTags() {
    return _config.getTags();
  }

  /**
   * Check if participant has a tag
   * @param tag tag to check
   * @return true if tagged, false otherwise
   */
  public boolean hasTag(String tag) {
    return _config.hasTag(tag);
  }

  /**
   * Get message map
   * @return message map
   */
  public Map<MessageId, Message> getMessageMap() {
    return _messageMap;
  }

  /**
   * Get the current states of the resource
   * @return map of resource-id to current state, or empty map if none
   */
  public Map<ResourceId, CurrentState> getCurrentStateMap() {
    return _currentStateMap;
  }

  /**
   * Get user-specified configuration properties of this participant
   * @return UserConfig properties
   */
  public UserConfig getUserConfig() {
    return _config.getUserConfig();
  }

  /**
   * Get the participant id
   * @return ParticipantId
   */
  public ParticipantId getId() {
    return _config.getId();
  }

  /**
   * Get the participant configuration
   * @return ParticipantConfig that backs this participant
   */
  public ParticipantConfig getConfig() {
    return _config;
  }
}

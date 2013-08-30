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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * A cluster participant
 */
public class Participant {
  private final ParticipantId _id;
  private final String _hostName;
  private final int _port;
  private final boolean _isEnabled;

  /**
   * set of disabled partition id's
   */
  private final Set<PartitionId> _disabledPartitionIdSet;
  private final Set<String> _tags;

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
      Map<ResourceId, CurrentState> currentStateMap, Map<MessageId, Message> messageMap) {
    _id = id;
    _hostName = hostName;
    _port = port;
    _isEnabled = isEnabled;
    _disabledPartitionIdSet = ImmutableSet.copyOf(disabledPartitionIdSet);
    _tags = ImmutableSet.copyOf(tags);
    _runningInstance = runningInstance;
    _currentStateMap = ImmutableMap.copyOf(currentStateMap);
    _messageMap = ImmutableMap.copyOf(messageMap);
  }

  /**
   * Get the host name of the participant
   * @return host name, or null if not applicable
   */
  public String getHostName() {
    return _hostName;
  }

  /**
   * Get the port of the participant
   * @return port number, or -1 if not applicable
   */
  public int getPort() {
    return _port;
  }

  /**
   * Get if the participant is enabled
   * @return true if enabled or false otherwise
   */
  public boolean isEnabled() {
    return _isEnabled;
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
    return _disabledPartitionIdSet;
  }

  /**
   * Get tags
   * @return set of tags
   */
  public Set<String> getTags() {
    return _tags;
  }

  /**
   * Check if participant has a tag
   * @param tag tag to check
   * @return true if tagged, false otherwise
   */
  public boolean hasTag(String tag) {
    return _tags.contains(tag);
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

  public ParticipantId getId() {
    return _id;
  }

  /**
   * Assemble a participant
   */
  public static class Builder {
    private final ParticipantId _id;
    private final Set<PartitionId> _disabledPartitions;
    private final Set<String> _tags;
    private final Map<ResourceId, CurrentState> _currentStateMap;
    private final Map<MessageId, Message> _messageMap;
    private String _hostName;
    private int _port;
    private boolean _isEnabled;
    private RunningInstance _runningInstance;

    /**
     * Build a participant with a given id
     * @param id participant id
     */
    public Builder(ParticipantId id) {
      _id = id;
      _disabledPartitions = new HashSet<PartitionId>();
      _tags = new HashSet<String>();
      _currentStateMap = new HashMap<ResourceId, CurrentState>();
      _messageMap = new HashMap<MessageId, Message>();
      _isEnabled = true;
    }

    /**
     * Set the participant host name
     * @param hostName reachable host when live
     * @return Builder
     */
    public Builder hostName(String hostName) {
      _hostName = hostName;
      return this;
    }

    /**
     * Set the participant port
     * @param port port number
     * @return Builder
     */
    public Builder port(int port) {
      _port = port;
      return this;
    }

    /**
     * Set whether or not the participant is enabled
     * @param isEnabled true if enabled, false otherwise
     * @return Builder
     */
    public Builder enabled(boolean isEnabled) {
      _isEnabled = isEnabled;
      return this;
    }

    /**
     * Add a partition to disable for this participant
     * @param partitionId the partition to disable
     * @return Builder
     */
    public Builder addDisabledPartition(PartitionId partitionId) {
      _disabledPartitions.add(partitionId);
      return this;
    }

    /**
     * Add an arbitrary tag for this participant
     * @param tag the tag to add
     * @return Builder
     */
    public Builder addTag(String tag) {
      _tags.add(tag);
      return this;
    }

    /**
     * Add live properties to participants that are running
     * @param runningInstance live participant properties
     * @return Builder
     */
    public Builder runningInstance(RunningInstance runningInstance) {
      _runningInstance = runningInstance;
      return this;
    }

    /**
     * Add a resource current state for this participant
     * @param resourceId the resource the current state corresponds to
     * @param currentState the current state
     * @return Builder
     */
    public Builder addCurrentState(ResourceId resourceId, CurrentState currentState) {
      _currentStateMap.put(resourceId, currentState);
      return this;
    }

    /**
     * Add a message for the participant
     * @param message message to add
     * @return Builder
     */
    public Builder addMessage(Message message) {
      _messageMap.put(new MessageId(message.getId()), message);
      return this;
    }

    /**
     * Assemble the participant
     * @return instantiated Participant
     */
    public Participant build() {
      return new Participant(_id, _hostName, _port, _isEnabled, _disabledPartitions, _tags,
          _runningInstance, _currentStateMap, _messageMap);
    }
  }
}

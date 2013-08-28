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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
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
  private final Set<PartitionId> _disabledPartitionIds;
  private final Set<String> _tags;

  private final RunningInstance _runningInstance;

  /**
   * map of resource-id to current-state
   */
  private final Map<ResourceId, CurState> _currentStateMap;

  /**
   * map of message-id to message
   */
  private final Map<MsgId, Msg> _messageMap;

  // TODO move this to ParticipantAccessor
  /**
   * Construct a participant
   * @param config
   */
  public Participant(ParticipantId id, InstanceConfig config, LiveInstance liveInstance,
      Map<String, CurrentState> currentStateMap, Map<String, Message> instanceMsgMap) {
    _id = id;
    _hostName = config.getHostName();

    int port = -1;
    try {
      port = Integer.parseInt(config.getPort());
    } catch (IllegalArgumentException e) {
      // keep as -1
    }
    if (port < 0 || port > 65535) {
      port = -1;
    }
    _port = port;
    _isEnabled = config.getInstanceEnabled();

    List<String> disabledPartitions = config.getDisabledPartitions();
    if (disabledPartitions == null) {
      _disabledPartitionIds = Collections.emptySet();
    } else {
      Set<PartitionId> disabledPartitionSet = new HashSet<PartitionId>();
      for (String partitionId : disabledPartitions) {
        disabledPartitionSet.add(new PartitionId(PartitionId.extracResourceId(partitionId),
            PartitionId.stripResourceId(partitionId)));
      }
      _disabledPartitionIds = ImmutableSet.copyOf(disabledPartitionSet);
    }

    List<String> tags = config.getTags();
    if (tags == null) {
      _tags = Collections.emptySet();
    } else {
      _tags = ImmutableSet.copyOf(config.getTags());
    }

    if (liveInstance != null) {
      _runningInstance =
          new RunningInstance(new SessionId(liveInstance.getSessionId()), new HelixVersion(
              liveInstance.getHelixVersion()), new ProcId(liveInstance.getLiveInstance()));
    } else {
      _runningInstance = null;
    }

    // TODO set curstate
    // Map<ParticipantId, CurState> curStateMap = new HashMap<ParticipantId, CurState>();
    // if (currentStateMap != null) {
    // for (String participantId : currentStateMap.keySet()) {
    // CurState curState =
    // new CurState(_id, new ParticipantId(participantId), currentStateMap.get(participantId));
    // curStateMap.put(new ParticipantId(participantId), curState);
    // }
    // }
    // _currentStateMap = ImmutableMap.copyOf(curStateMap);
    _currentStateMap = null;

    Map<MsgId, Msg> msgMap = new HashMap<MsgId, Msg>();
    for (String msgId : instanceMsgMap.keySet()) {
      Message message = instanceMsgMap.get(msgId);
      msgMap.put(new MsgId(msgId), new Msg(message));
    }
    _messageMap = ImmutableMap.copyOf(msgMap);

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
    return _disabledPartitionIds;
  }

  /**
   * Get tags
   * @return set of tags
   */
  public Set<String> getTags() {
    return _tags;
  }

  /**
   * Get message map
   * @return message map
   */
  public Map<MsgId, Msg> getMessageMap() {
    return _messageMap;
  }

  /**
   * Get the current states of the resource
   * @return map of resource-id to current state, or empty map if none
   */
  public Map<ResourceId, CurState> getCurrentStateMap() {
    return _currentStateMap;
  }
}

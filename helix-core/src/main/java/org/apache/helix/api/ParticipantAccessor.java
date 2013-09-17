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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

public class ParticipantAccessor {
  private static final Logger LOG = Logger.getLogger(ParticipantAccessor.class);

  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;
  private final ClusterId _clusterId;

  public ParticipantAccessor(ClusterId clusterId, HelixDataAccessor accessor) {
    _clusterId = clusterId;
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
  }

  /**
   * enable/disable a participant
   * @param participantId
   * @param isEnabled
   */
  void enableParticipant(ParticipantId participantId, boolean isEnabled) {
    String participantName = participantId.stringify();
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantName)) == null) {
      throw new HelixException("Config for participant: " + participantId
          + " does NOT exist in cluster: " + _clusterId);
    }

    InstanceConfig config = new InstanceConfig(participantName);
    config.setInstanceEnabled(isEnabled);
    _accessor.updateProperty(_keyBuilder.instanceConfig(participantName), config);

  }

  /**
   * disable participant
   * @param participantId
   */
  public void disableParticipant(ParticipantId participantId) {
    enableParticipant(participantId, false);
  }

  /**
   * enable participant
   * @param participantId
   */
  public void enableParticipant(ParticipantId participantId) {
    enableParticipant(participantId, true);
  }

  /**
   * create messages for participant
   * @param participantId
   * @param msgMap map of message-id to message
   */
  public void insertMessagesToParticipant(ParticipantId participantId,
      Map<MessageId, Message> msgMap) {
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    List<Message> msgs = new ArrayList<Message>();
    for (MessageId msgId : msgMap.keySet()) {
      msgKeys.add(_keyBuilder.message(participantId.stringify(), msgId.stringify()));
      msgs.add(msgMap.get(msgId));
    }

    _accessor.createChildren(msgKeys, msgs);
  }

  /**
   * set messages of participant
   * @param participantId
   * @param msgMap map of message-id to message
   */
  public void updateMessageStatus(ParticipantId participantId, Map<MessageId, Message> msgMap) {
    String participantName = participantId.stringify();
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    List<Message> msgs = new ArrayList<Message>();
    for (MessageId msgId : msgMap.keySet()) {
      msgKeys.add(_keyBuilder.message(participantName, msgId.stringify()));
      msgs.add(msgMap.get(msgId));
    }
    _accessor.setChildren(msgKeys, msgs);
  }

  /**
   * delete messages from participant
   * @param participantId
   * @param msgIdSet
   */
  public void deleteMessagesFromParticipant(ParticipantId participantId, Set<MessageId> msgIdSet) {
    String participantName = participantId.stringify();
    List<PropertyKey> msgKeys = new ArrayList<PropertyKey>();
    for (MessageId msgId : msgIdSet) {
      msgKeys.add(_keyBuilder.message(participantName, msgId.stringify()));
    }

    // TODO impl batch remove
    for (PropertyKey msgKey : msgKeys) {
      _accessor.removeProperty(msgKey);
    }
  }

  /**
   * enable/disable partitions on a participant
   * @param enabled
   * @param participantId
   * @param resourceId
   * @param partitionIdSet
   */
  void enablePartitionsForParticipant(final boolean enabled, final ParticipantId participantId,
      final ResourceId resourceId, final Set<PartitionId> partitionIdSet) {
    String participantName = participantId.stringify();
    String resourceName = resourceId.stringify();

    // check instanceConfig exists
    PropertyKey instanceConfigKey = _keyBuilder.instanceConfig(participantName);
    if (_accessor.getProperty(instanceConfigKey) == null) {
      throw new HelixException("Config for participant: " + participantId
          + " does NOT exist in cluster: " + _clusterId);
    }

    // check resource exist. warn if not
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealState(resourceName));
    if (idealState == null) {
      LOG.warn("Disable partitions: " + partitionIdSet + " but Cluster: " + _clusterId
          + ", resource: " + resourceId
          + " does NOT exists. probably disable it during ERROR->DROPPED transtition");

    } else {
      // check partitions exist. warn if not
      for (PartitionId partitionId : partitionIdSet) {
        String partitionName = partitionId.stringify();
        if ((idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO && idealState
            .getPreferenceList(partitionName) == null)
            || (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED && idealState
                .getInstanceStateMap(partitionName) == null)) {
          LOG.warn("Cluster: " + _clusterId + ", resource: " + resourceId + ", partition: "
              + partitionId + ", partition does NOT exist in ideal state");
        }
      }
    }

    // TODO merge list logic should go to znrecord updater
    // update participantConfig
    // could not use ZNRecordUpdater since it doesn't do listField merge/subtract
    BaseDataAccessor<ZNRecord> baseAccessor = _accessor.getBaseDataAccessor();
    final List<String> partitionNames = new ArrayList<String>();
    for (PartitionId partitionId : partitionIdSet) {
      partitionNames.add(partitionId.stringify());
    }

    baseAccessor.update(instanceConfigKey.getPath(), new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + _clusterId + ", instance: " + participantId
              + ", participant config is null");
        }

        // TODO: merge with InstanceConfig.setInstanceEnabledForPartition
        List<String> list =
            currentData.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
        Set<String> disabledPartitions = new HashSet<String>();
        if (list != null) {
          disabledPartitions.addAll(list);
        }

        if (enabled) {
          disabledPartitions.removeAll(partitionNames);
        } else {
          disabledPartitions.addAll(partitionNames);
        }

        list = new ArrayList<String>(disabledPartitions);
        Collections.sort(list);
        currentData.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(), list);
        return currentData;
      }
    }, AccessOption.PERSISTENT);
  }

  /**
   * disable partitions on a participant
   * @param participantId
   * @param resourceId
   * @param disablePartitionIdSet
   */
  public void disablePartitionsForParticipant(ParticipantId participantId, ResourceId resourceId,
      Set<PartitionId> disablePartitionIdSet) {
    enablePartitionsForParticipant(false, participantId, resourceId, disablePartitionIdSet);
  }

  /**
   * enable partitions on a participant
   * @param participantId
   * @param resourceId
   * @param enablePartitionIdSet
   */
  public void enablePartitionsForParticipant(ParticipantId participantId, ResourceId resourceId,
      Set<PartitionId> enablePartitionIdSet) {
    enablePartitionsForParticipant(true, participantId, resourceId, enablePartitionIdSet);
  }

  /**
   * reset partitions on a participant
   * @param participantId
   * @param resourceId
   * @param resetPartitionIdSet
   */
  public void resetPartitionsForParticipant(ParticipantId participantId, ResourceId resourceId,
      Set<PartitionId> resetPartitionIdSet) {
    // TODO impl this
  }

  /**
   * create live instance for the participant
   * @param participantId
   */
  public void connectParticipant(ParticipantId participantId) {
    // TODO impl this
  }

  /**
   * create a participant based on physical model
   * @param participantId
   * @param instanceConfig
   * @param userConfig
   * @param liveInstance
   * @param instanceMsgMap map of message-id to message
   * @param instanceCurStateMap map of resource-id to current-state
   * @return participant
   */
  static Participant createParticipant(ParticipantId participantId, InstanceConfig instanceConfig,
      UserConfig userConfig, LiveInstance liveInstance, Map<String, Message> instanceMsgMap,
      Map<String, CurrentState> instanceCurStateMap) {

    String hostName = instanceConfig.getHostName();

    int port = -1;
    try {
      port = Integer.parseInt(instanceConfig.getPort());
    } catch (IllegalArgumentException e) {
      // keep as -1
    }
    if (port < 0 || port > 65535) {
      port = -1;
    }
    boolean isEnabled = instanceConfig.getInstanceEnabled();

    List<String> disabledPartitions = instanceConfig.getDisabledPartitions();
    Set<PartitionId> disabledPartitionIdSet = Collections.emptySet();
    if (disabledPartitions != null) {
      disabledPartitionIdSet = new HashSet<PartitionId>();
      for (String partitionId : disabledPartitions) {
        disabledPartitionIdSet.add(PartitionId.from(PartitionId.extractResourceId(partitionId),
            PartitionId.stripResourceId(partitionId)));
      }
    }

    Set<String> tags = new HashSet<String>(instanceConfig.getTags());

    RunningInstance runningInstance = null;
    if (liveInstance != null) {
      runningInstance =
          new RunningInstance(liveInstance.getSessionId(), liveInstance.getHelixVersion(),
              liveInstance.getProcessId());
    }

    Map<MessageId, Message> msgMap = new HashMap<MessageId, Message>();
    if (instanceMsgMap != null) {
      for (String msgId : instanceMsgMap.keySet()) {
        Message message = instanceMsgMap.get(msgId);
        msgMap.put(MessageId.from(msgId), message);
      }
    }

    Map<ResourceId, CurrentState> curStateMap = new HashMap<ResourceId, CurrentState>();
    if (instanceCurStateMap != null) {

      for (String resourceName : instanceCurStateMap.keySet()) {
        curStateMap.put(ResourceId.from(resourceName), instanceCurStateMap.get(resourceName));
      }
    }

    return new Participant(participantId, hostName, port, isEnabled, disabledPartitionIdSet, tags,
        runningInstance, curStateMap, msgMap, userConfig);
  }

  /**
   * read participant related data
   * @param participantId
   * @return participant
   */
  public Participant readParticipant(ParticipantId participantId) {
    // read physical model
    String participantName = participantId.stringify();
    InstanceConfig instanceConfig = _accessor.getProperty(_keyBuilder.instance(participantName));
    UserConfig userConfig = UserConfig.from(instanceConfig);
    LiveInstance liveInstance = _accessor.getProperty(_keyBuilder.liveInstance(participantName));

    Map<String, Message> instanceMsgMap = Collections.emptyMap();
    Map<String, CurrentState> instanceCurStateMap = Collections.emptyMap();
    if (liveInstance != null) {
      SessionId sessionId = liveInstance.getSessionId();

      instanceMsgMap = _accessor.getChildValuesMap(_keyBuilder.messages(participantName));
      instanceCurStateMap =
          _accessor.getChildValuesMap(_keyBuilder.currentStates(participantName,
              sessionId.stringify()));
    }

    return createParticipant(participantId, instanceConfig, userConfig, liveInstance,
        instanceMsgMap, instanceCurStateMap);
  }

  /**
   * update resource current state of a participant
   * @param resourceId resource id
   * @param participantId participant id
   * @param sessionId session id
   * @param curStateUpdate current state change delta
   */
  public void updateCurrentState(ResourceId resourceId, ParticipantId participantId,
      SessionId sessionId, CurrentState curStateUpdate) {
    _accessor.updateProperty(
        _keyBuilder.currentState(participantId.stringify(), sessionId.stringify(),
            resourceId.stringify()), curStateUpdate);
  }

  /**
   * drop resource current state of a participant
   * @param resourceId resource id
   * @param participantId participant id
   * @param sessionId session id
   */
  public void dropCurrentState(ResourceId resourceId, ParticipantId participantId,
      SessionId sessionId) {
    _accessor.removeProperty(_keyBuilder.currentState(participantId.stringify(),
        sessionId.stringify(), resourceId.stringify()));
  }
}

package org.apache.helix.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.UUID;

import org.apache.helix.HelixManager;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Message utils to operate on message such creating messages.
 */
public class MessageUtil {
  private static final Logger LOG = LoggerFactory.getLogger(MessageUtil.class);

  // TODO: Make the message retry count configurable through the Cluster Config or IdealStates.
  public final static int DEFAULT_STATE_TRANSITION_MESSAGE_RETRY_COUNT = 3;

  public static Message createStateTransitionCancellationMessage(HelixManager manager,
      Resource resource, String partitionName, String instanceName, String sessionId,
      String stateModelDefName, String fromState, String toState, String nextState,
      Message cancellationMessage, boolean isCancellationEnabled, String currentState) {
    if (isCancellationEnabled && cancellationMessage == null) {
      LOG.info("Create cancellation message of the state transition for {}.{} on {}, "
              + "currentState: {}, nextState: {},  toState: {}", resource.getResourceName(),
          partitionName, instanceName, currentState, nextState == null ? "N/A" : nextState,
          toState);

      Message message =
          createStateTransitionMessage(Message.MessageType.STATE_TRANSITION_CANCELLATION, manager,
              resource, partitionName, instanceName, currentState, nextState, sessionId,
              stateModelDefName);

      message.setFromState(fromState);
      message.setToState(toState);
      return message;
    }

    return null;
  }

  public static Message createStateTransitionMessage(HelixManager manager, Resource resource,
      String partitionName, String instanceName, String currentState, String nextState,
      String sessionId, String stateModelDefName) {
    Message message =
        createStateTransitionMessage(Message.MessageType.STATE_TRANSITION, manager, resource,
            partitionName, instanceName, currentState, nextState, sessionId, stateModelDefName);

    // Set the retry count for state transition messages.
    // TODO: make the retry count configurable in ClusterConfig or IdealState
    message.setRetryCount(DEFAULT_STATE_TRANSITION_MESSAGE_RETRY_COUNT);

    if (resource.getResourceGroupName() != null) {
      message.setResourceGroupName(resource.getResourceGroupName());
    }
    if (resource.getResourceTag() != null) {
      message.setResourceTag(resource.getResourceTag());
    }

    return message;
  }

  /**
   * Creates a message to change participant status
   * {@link org.apache.helix.model.LiveInstance.LiveInstanceStatus}
   *
   * @param currentState current status of the live instance
   * @param nextState next status that will be changed to
   * @param manager Helix manager
   * @param instanceName target instance name
   * @param sessionId target instance session id
   * @return participant status change message
   */
  public static Message createStatusChangeMessage(LiveInstance.LiveInstanceStatus currentState,
      LiveInstance.LiveInstanceStatus nextState, HelixManager manager, String instanceName,
      String sessionId) {
    return createBasicMessage(Message.MessageType.PARTICIPANT_STATUS_CHANGE, manager, instanceName,
        currentState.toString(), nextState.name(), sessionId);
  }

  /* Creates a message that that has the least required fields. */
  private static Message createBasicMessage(Message.MessageType messageType, HelixManager manager,
      String instanceName, String currentState, String nextState, String sessionId) {
    String uuid = UUID.randomUUID().toString();
    String managerSessionId = manager.getSessionId();

    Message message = new Message(messageType, uuid);
    message.setSrcName(manager.getInstanceName());
    message.setTgtName(instanceName);
    message.setMsgState(Message.MessageState.NEW);
    message.setFromState(currentState);
    message.setToState(nextState);
    message.setTgtSessionId(sessionId);
    message.setSrcSessionId(managerSessionId);
    message.setExpectedSessionId(managerSessionId);

    return message;
  }

  /* Creates state transition or state transition cancellation message */
  private static Message createStateTransitionMessage(Message.MessageType messageType,
      HelixManager manager, Resource resource, String partitionName, String instanceName,
      String currentState, String nextState, String sessionId, String stateModelDefName) {
    Message message =
        createBasicMessage(messageType, manager, instanceName, currentState, nextState, sessionId);
    message.setPartitionName(partitionName);
    message.setStateModelDef(stateModelDefName);
    message.setResourceName(resource.getResourceName());
    message.setStateModelFactoryName(resource.getStateModelFactoryname());
    message.setBucketSize(resource.getBucketSize());

    return message;
  }
}

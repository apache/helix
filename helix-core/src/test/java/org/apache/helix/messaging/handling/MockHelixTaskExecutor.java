package org.apache.helix.messaging.handling;

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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.Message;
import org.apache.helix.monitoring.mbeans.MessageQueueMonitor;
import org.apache.helix.monitoring.mbeans.ParticipantStatusMonitor;

public class MockHelixTaskExecutor extends HelixTaskExecutor {
  public static int duplicatedMessages = 0;
  public static int extraStateTransition = 0;
  public static int duplicatedMessagesInProgress = 0;
  HelixManager manager;

  public MockHelixTaskExecutor(ParticipantStatusMonitor participantStatusMonitor,
      MessageQueueMonitor messageQueueMonitor) {
    super(participantStatusMonitor, messageQueueMonitor);
  }

  @Override
  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext) {
    manager = changeContext.getManager();
    checkDuplicatedMessages(messages);
    super.onMessage(instanceName, messages, changeContext);
  }

  void checkDuplicatedMessages(List<Message> messages) {
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    PropertyKey path = keyBuilder.currentStates(manager.getInstanceName(), manager.getSessionId());
    Map<String, CurrentState> currentStateMap = accessor.getChildValuesMap(path);

    Set<String> seenPartitions = new HashSet<>();
    for (Message message : messages) {
      if (message.getMsgType().equals(Message.MessageType.STATE_TRANSITION.name())) {
        String resource = message.getResourceName();
        String partition = message.getPartitionName();

        //System.err.println(message.getMsgId());
        String key = resource + "-" + partition;
        if (seenPartitions.contains(key)) {
          //System.err.println("Duplicated message received for " + resource + ":" + partition);
          duplicatedMessages++;
        }
        seenPartitions.add(key);

        String toState = message.getToState();
        String state = null;
        if (currentStateMap.containsKey(resource)) {
          CurrentState currentState = currentStateMap.get(resource);
          state = currentState.getState(partition);
        }

        if (toState.equals(state) && message.getMsgState() == Message.MessageState.NEW) {
          //            logger.error(
          //                "Extra message: " + message.getMsgId() + ", Partition is already in target state "
          //                    + toState + " for " + resource + ":" + partition);
          extraStateTransition++;
        }

        String messageTarget =
            getMessageTarget(message.getResourceName(), message.getPartitionName());

        if (message.getMsgState() == Message.MessageState.NEW &&
            _messageTaskMap.containsKey(messageTarget)) {
          String taskId = _messageTaskMap.get(messageTarget);
          MessageTaskInfo messageTaskInfo = _taskMap.get(taskId);
          Message existingMsg = messageTaskInfo.getTask().getMessage();
          if (existingMsg.getMsgId() != message.getMsgId())
            //            logger.error("Duplicated message In Progress: " + message.getMsgId()
            //                    + ", state transition in progress with message " + existingMsg.getMsgId()
            //                    + " to " + toState + " for " + resource + ":" + partition);
            duplicatedMessagesInProgress ++;
        }
      }
    }
  }

  public static void resetStats() {
    duplicatedMessages = 0;
    extraStateTransition = 0;
    duplicatedMessagesInProgress = 0;
  }
}

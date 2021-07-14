package org.apache.helix.controller.stages;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.LiveInstance.LiveInstanceStatus;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.util.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates participant status change (freeze/unfreeze) and pending state transition cancellation
 * messages for management mode pipeline.
 */
public class ManagementMessageGenerationPhase extends MessageGenerationPhase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementMessageGenerationPhase.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    _eventId = event.getEventId();
    String clusterName = event.getClusterName();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    ClusterManagementMode managementMode = event.getAttribute(AttributeName.CLUSTER_STATUS.name());
    ManagementControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    if (manager == null || managementMode == null || cache == null) {
      throw new StageException("Missing attributes in event: " + event
          + ". Requires HelixManager|ClusterStatus|DataCache");
    }

    PauseSignal pauseSignal = cache.getPauseSignal();
    if (cache.getClusterConfig().isStateTransitionCancelEnabled()
        && pauseSignal != null && pauseSignal.getCancelPendingST()) {
      // Generate ST cancellation messages.
      LogUtil.logInfo(LOG, _eventId,
          "Generating ST cancellation messages for cluster " + clusterName);
      super.process(event);
    }

    MessageOutput messageOutput =
        event.getAttributeWithDefault(AttributeName.MESSAGES_ALL.name(), new MessageOutput());
    // Is participant status change still in progress? Create messages
    if (!ClusterManagementMode.Status.COMPLETED.equals(managementMode.getStatus())) {
      LogUtil.logInfo(LOG, _eventId, "Generating messages as cluster " + clusterName
          + " is still in progress to change participant status");
      List<Message> messages = generateStatusChangeMessages(managementMode,
          cache.getEnabledLiveInstances(), cache.getLiveInstances(),
          cache.getAllInstancesMessages(), manager.getInstanceName(), manager.getSessionId());
      messageOutput.addStatusChangeMessages(messages);
    }

    event.addAttribute(AttributeName.MESSAGES_ALL.name(), messageOutput);
  }

  private List<Message> generateStatusChangeMessages(ClusterManagementMode managementMode,
      Set<String> enabledLiveInstances, Map<String, LiveInstance> liveInstanceMap,
      Map<String, Collection<Message>> allInstanceMessages, String managerInstance,
      String managerSessionId) {
    List<Message> messagesGenerated = new ArrayList<>();

    LiveInstanceStatus desiredStatus = managementMode.getDesiredParticipantStatus();

    // Check status and pending status change messages for all enabled live instances.
    // Send freeze/unfreeze messages if necessary
    for (String instanceName : enabledLiveInstances) {
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      Collection<Message> pendingMessages = allInstanceMessages.get(instanceName);
      String sessionId = liveInstance.getEphemeralOwner();
      LiveInstanceStatus currentStatus = liveInstance.getStatus();

      if (needStatusChangeMessage(pendingMessages, currentStatus, desiredStatus)) {
        Message statusChangeMessage = MessageUtil.createStatusChangeMessage(currentStatus,
            desiredStatus, managerInstance, managerSessionId, instanceName, sessionId);
        messagesGenerated.add(statusChangeMessage);
      }
    }

    return messagesGenerated;
  }

  private boolean needStatusChangeMessage(Collection<Message> messages,
      LiveInstanceStatus currentStatus, LiveInstanceStatus desiredStatus) {
    // 1. current status is not equal to desired status
    // 2. participant change status message is not sent
    return currentStatus != desiredStatus && messages.stream().noneMatch(
        message -> message.isParticipantStatusChangeType() && desiredStatus.name()
            .equals(message.getToState()));
  }
}

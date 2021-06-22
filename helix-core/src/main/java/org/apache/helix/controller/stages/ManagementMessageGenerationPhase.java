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

    // Is participant status change still in progress? Create messages
    if (!ClusterManagementMode.Status.COMPLETED.equals(managementMode.getStatus())) {
      LogUtil.logInfo(LOG, _eventId, "Generating messages as cluster " + clusterName
          + " is still in progress to change participant status");
      List<Message> messages =
          generateStatusChangeMessages(managementMode.getMode(), cache.getEnabledLiveInstances(),
              cache.getLiveInstances(), cache.getAllInstancesMessages(), manager.getInstanceName(),
              manager.getSessionId());
      MessageOutput messageOutput =
          event.getAttributeWithDefault(AttributeName.MESSAGES_ALL.name(), new MessageOutput());
      messageOutput.addStatusChangeMessages(messages);
      event.addAttribute(AttributeName.MESSAGES_ALL.name(), messageOutput);
    }
  }

  private List<Message> generateStatusChangeMessages(ClusterManagementMode.Type managementMode,
      Set<String> enabledLiveInstances, Map<String, LiveInstance> liveInstanceMap,
      Map<String, Collection<Message>> allInstanceMessages, String managerInstance,
      String managerSessionId) {
    List<Message> messagesToSend = new ArrayList<>();

    LiveInstanceStatus fromStatus = null;
    LiveInstanceStatus toStatus = null;
    if (ClusterManagementMode.Type.CLUSTER_PAUSE.equals(managementMode)) {
      fromStatus = LiveInstanceStatus.NORMAL;
      toStatus = LiveInstanceStatus.PAUSED;
    } else if (ClusterManagementMode.Type.NORMAL.equals(managementMode)) {
      fromStatus = LiveInstanceStatus.PAUSED;
      toStatus = LiveInstanceStatus.NORMAL;
    }

    // Check status and pending status change messages for all enabled live instances
    // Send freeze/unfreeze messages if necessary
    for (String instanceName : enabledLiveInstances) {
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      Collection<Message> messages = allInstanceMessages.get(instanceName);
      String sessionId = liveInstance.getEphemeralOwner();

      if (needStatusChangeMessage(messages, liveInstance.getStatus(), fromStatus, toStatus)) {
        Message statusChangeMessage = MessageUtil.createStatusChangeMessage(fromStatus, toStatus,
            managerInstance, managerSessionId, instanceName, sessionId);
        messagesToSend.add(statusChangeMessage);
      }
    }

    return messagesToSend;
  }

  private boolean needStatusChangeMessage(Collection<Message> messages,
      LiveInstanceStatus currentStatus, LiveInstanceStatus fromStatus,
      LiveInstanceStatus toStatus) {
    if (fromStatus == null || toStatus == null || currentStatus == toStatus) {
      return false;
    }
    // Is participant change status message not sent?
    return messages.stream().noneMatch(
        message -> message.isParticipantStatusChangeType() && toStatus.name()
            .equals(message.getToState()));
  }
}

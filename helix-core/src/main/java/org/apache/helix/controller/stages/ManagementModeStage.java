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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixManager;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes necessary cluster events for cluster management mode.
 */
public class ManagementModeStage extends AbstractBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementModeStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
  }

  private ClusterManagementMode checkClusterFreezeStatus(HelixManager manager,
      ManagementControllerDataProvider cache, List<Message> messagesToSend) {
    PauseSignal pauseSignal = cache.getPauseSignal();
    if (pauseSignal != null && pauseSignal.isClusterPause()) {
      return checkFreezeStatus(manager, cache, messagesToSend, pauseSignal.getCancelPendingST());
    }
    return checkUnfreezeStatus(manager, cache, messagesToSend);
  }

  private ClusterManagementMode checkFreezeStatus(HelixManager manager,
      ManagementControllerDataProvider cache, List<Message> messagesToSend,
      boolean cancelPendingST) {
    Set<String> enabledLiveInstances = cache.getEnabledLiveInstances();
    Map<String, LiveInstance> liveInstanceMap = cache.getLiveInstances();
    // Map: resource -> partition -> instance -> message
    Map<String, Map<String, Map<String, Message>>> cancelMessageMap = buildCancelMessageMap(cache);

    boolean cancellationEnabled = cache.getClusterConfig().isStateTransitionCancelEnabled();
    ClusterManagementMode.Status status = ClusterManagementMode.Status.COMPLETED;

    // Check status and pending ST for all enabled live instances
    // Send freeze/cancellation messages if necessary
    for (String instanceName : enabledLiveInstances) {
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      Collection<Message> messages = cache.getMessages(instanceName).values();
      String sessionId = cache.getLiveInstances().get(instanceName).getEphemeralOwner();
      boolean isFrozen = (LiveInstance.LiveInstanceStatus.PAUSED.equals(liveInstance.getStatus()));

      if (!isFrozen) {
        status = ClusterManagementMode.Status.IN_PROGRESS;
        // Has the freeze message been sent?
        boolean isStatusChangeMessageSent =
            isStatusChangeMessageExisted(messages, LiveInstance.LiveInstanceStatus.PAUSED);
        if (!isStatusChangeMessageSent) {
          Message message = createStatusChangeMessage(manager.getInstanceName(), instanceName,
              LiveInstance.LiveInstanceStatus.PAUSED, sessionId, manager.getSessionId());
          messagesToSend.add(message);
        }
      }

      // Handle pending ST
      if (hasPendingST(messages)) {
        status = ClusterManagementMode.Status.IN_PROGRESS;
        if (cancellationEnabled && cancelPendingST) {
          List<Message> cancelMessages =
              createCancellationMessages(messages, manager.getSessionId(),
                  manager.getInstanceName(), sessionId, cancelMessageMap);
          messagesToSend.addAll(cancelMessages);
        }
      }
    }

    return new ClusterManagementMode(ClusterManagementMode.Type.CLUSTER_PAUSE, status);
  }

  private ClusterManagementMode checkUnfreezeStatus(HelixManager manager,
      ManagementControllerDataProvider cache, List<Message> messagesToSend) {
    Set<String> enabledLiveInstances = cache.getEnabledLiveInstances();
    Map<String, LiveInstance> liveInstanceMap = cache.getLiveInstances();
    ClusterManagementMode.Status status = ClusterManagementMode.Status.COMPLETED;

    // Check paused status for all enabled live instances
    // Send pause/cancellation messages if necessary
    for (String instanceName : enabledLiveInstances) {
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      Collection<Message> messages = cache.getMessages(instanceName).values();
      String sessionId = cache.getLiveInstances().get(instanceName).getEphemeralOwner();
      boolean isNormalStatus = (liveInstance.getStatus() == null);

      if (!isNormalStatus) {
        // Still in progress of unfreeze.
        status = ClusterManagementMode.Status.IN_PROGRESS;
        // Has the unfreeze message been sent?
        boolean statusChangeMessageExisted =
            isStatusChangeMessageExisted(messages, LiveInstance.LiveInstanceStatus.NORMAL);
        if (!statusChangeMessageExisted) {
          Message statusChangeMessage =
              createStatusChangeMessage(manager.getInstanceName(), instanceName,
                  LiveInstance.LiveInstanceStatus.NORMAL, sessionId, manager.getSessionId());
          messagesToSend.add(statusChangeMessage);
        }
      }
    }

    return new ClusterManagementMode(ClusterManagementMode.Type.NORMAL, status);
  }

  private boolean hasPendingST(Collection<Message> messages) {
    return messages.stream().anyMatch(message -> Message.MessageType.STATE_TRANSITION.name()
        .equalsIgnoreCase(message.getMsgType()));
  }

  private boolean isStatusChangeMessageExisted(Collection<Message> messages,
      LiveInstance.LiveInstanceStatus toStatus) {
    return messages.stream().anyMatch(msg ->
        Message.MessageType.PARTICIPANT_STATUS_CHANGE.name().equalsIgnoreCase(msg.getMsgType())
            && toStatus.equals(msg.getToStatus()));
  }
}

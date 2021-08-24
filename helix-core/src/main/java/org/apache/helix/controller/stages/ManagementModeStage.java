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

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.helix.model.ClusterStatus;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.LiveInstance.LiveInstanceStatus;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.Resource;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.RebalanceUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks the cluster status whether the cluster is in management mode.
 */
public class ManagementModeStage extends AbstractBaseStage {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementModeStage.class);

  // Check message types to determine instance is in progress to be frozen
  private static final Set<String> PENDING_MESSAGE_TYPES = ImmutableSet.of(
      MessageType.PARTICIPANT_STATUS_CHANGE.name(),
      MessageType.STATE_TRANSITION.name(),
      MessageType.STATE_TRANSITION_CANCELLATION.name()
  );

  @Override
  public void process(ClusterEvent event) throws Exception {
    // TODO: implement the stage
    _eventId = event.getEventId();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      throw new StageException("HelixManager attribute value is null");
    }

    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ManagementControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    CurrentStateOutput currentStateOutput =
        event.getAttribute(AttributeName.CURRENT_STATE.name());
    final Map<String, Resource> resourceMap =
        event.getAttribute(AttributeName.RESOURCES_TO_REBALANCE.name());

    final BestPossibleStateOutput bestPossibleStateOutput =
        RebalanceUtil.buildBestPossibleState(resourceMap.keySet(), currentStateOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);

    ClusterManagementMode managementMode =
        checkClusterFreezeStatus(cache.getEnabledLiveInstances(), cache.getLiveInstances(),
            cache.getAllInstancesMessages(), cache.getPauseSignal());

    recordClusterStatus(managementMode, cache.getPauseSignal(), manager.getInstanceName(), accessor);

    event.addAttribute(AttributeName.CLUSTER_STATUS.name(), managementMode);
  }

  // Checks cluster freeze, controller pause mode and status.
  private ClusterManagementMode checkClusterFreezeStatus(
      Set<String> enabledLiveInstances,
      Map<String, LiveInstance> liveInstanceMap,
      Map<String, Collection<Message>> allInstanceMessages,
      PauseSignal pauseSignal) {
    ClusterManagementMode.Type type;
    ClusterManagementMode.Status status = ClusterManagementMode.Status.COMPLETED;
    if (pauseSignal == null) {
      // TODO: Should check maintenance mode after it's moved to management pipeline.
      type = ClusterManagementMode.Type.NORMAL;
      if (HelixUtil.inManagementMode(pauseSignal, liveInstanceMap, enabledLiveInstances,
          allInstanceMessages)) {
        status = ClusterManagementMode.Status.IN_PROGRESS;
      }
    } else if (pauseSignal.isClusterPause()) {
      type = ClusterManagementMode.Type.CLUSTER_FREEZE;
      if (!instancesFullyFrozen(enabledLiveInstances, liveInstanceMap, allInstanceMessages)) {
        status = ClusterManagementMode.Status.IN_PROGRESS;
      }
    } else {
      type = ClusterManagementMode.Type.CONTROLLER_PAUSE;
    }

    return new ClusterManagementMode(type, status);
  }

  private boolean instancesFullyFrozen(Set<String> enabledLiveInstances,
      Map<String, LiveInstance> liveInstanceMap,
      Map<String, Collection<Message>> allInstanceMessages) {
    // 1. All live instances are frozen
    // 2. No pending participant status change message.
    return enabledLiveInstances.stream().noneMatch(
        instance -> !LiveInstanceStatus.FROZEN.equals(liveInstanceMap.get(instance).getStatus())
            || hasPendingMessage(allInstanceMessages.get(instance)));
  }

  private boolean hasPendingMessage(Collection<Message> messages) {
    return messages != null && messages.stream()
        .anyMatch(message -> PENDING_MESSAGE_TYPES.contains(message.getMsgType()));
  }

  private void recordClusterStatus(ClusterManagementMode mode, PauseSignal pauseSignal,
      String controllerName, HelixDataAccessor accessor) {
    // Read cluster status from metadata store
    PropertyKey statusPropertyKey = accessor.keyBuilder().clusterStatus();
    ClusterStatus clusterStatus = accessor.getProperty(statusPropertyKey);
    if (clusterStatus == null) {
      clusterStatus = new ClusterStatus();
    }

    if (mode.getMode().equals(clusterStatus.getManagementMode())
        && mode.getStatus().equals(clusterStatus.getManagementModeStatus())) {
      // No need to update status and avoid duplicates when it's the same as metadata store
      LOG.debug("Skip recording duplicate status mode={}, status={}", mode.getMode(),
          mode.getStatus());
      return;
    }

    // Update cluster status in metadata store
    clusterStatus.setManagementMode(mode.getMode());
    clusterStatus.setManagementModeStatus(mode.getStatus());
    if (!accessor.updateProperty(statusPropertyKey, clusterStatus)) {
      LOG.error("Failed to update cluster status {}", clusterStatus);
    }

    recordManagementModeHistory(mode, pauseSignal, controllerName, accessor);
  }

  private void recordManagementModeHistory(ClusterManagementMode mode, PauseSignal pauseSignal,
      String controllerName, HelixDataAccessor accessor) {
    // Only record completed status
    if (!ClusterManagementMode.Status.COMPLETED.equals(mode.getStatus())) {
      return;
    }

    // Record a management mode history in controller history
    String path = accessor.keyBuilder().controllerLeaderHistory().getPath();
    long timestamp = Instant.now().toEpochMilli();
    String fromHost = (pauseSignal == null ? null : pauseSignal.getFromHost());
    String reason = (pauseSignal == null ? null : pauseSignal.getReason());

    // Need the updater to avoid race condition with controller/maintenance history updates.
    if (!accessor.getBaseDataAccessor().update(path, oldRecord -> {
      if (oldRecord == null) {
        oldRecord = new ZNRecord(PropertyType.HISTORY.toString());
      }
      return new ControllerHistory(oldRecord)
          .updateManagementModeHistory(controllerName, mode, fromHost, timestamp, reason);
    }, AccessOption.PERSISTENT)) {
      LOG.error("Failed to write management mode history to ZK!");
    }
  }
}

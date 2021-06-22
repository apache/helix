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

import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.controller.LogUtil;
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

  @Override
  public void process(ClusterEvent event) throws Exception {
    // TODO: implement the stage
    _eventId = event.getEventId();
    String clusterName = event.getClusterName();
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (manager == null) {
      throw new StageException("HelixManager attribute value is null");
    }

    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    ManagementControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

    ClusterManagementMode managementMode =
        checkClusterFreezeStatus(cache.getEnabledLiveInstances(), cache.getLiveInstances(),
            cache.getAllInstancesMessages(), cache.getPauseSignal());

    recordClusterStatus(managementMode, accessor);
    recordManagementModeHistory(managementMode, cache.getPauseSignal(), manager.getInstanceName(),
        accessor);

    // TODO: move to the last stage of management pipeline
    checkInManagementMode(clusterName, cache);
  }

  private void checkInManagementMode(String clusterName, ManagementControllerDataProvider cache) {
    // Should exit management mode
    if (!HelixUtil.inManagementMode(cache.getPauseSignal(), cache.getLiveInstances(),
        cache.getEnabledLiveInstances(), cache.getAllInstancesMessages())) {
      LogUtil.logInfo(LOG, _eventId, "Exiting management mode pipeline for cluster " + clusterName);
      RebalanceUtil.enableManagementMode(clusterName, false);
    }
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
      type = ClusterManagementMode.Type.CLUSTER_PAUSE;
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
        instance -> !LiveInstanceStatus.PAUSED.equals(liveInstanceMap.get(instance).getStatus())
            || hasPendingMessage(allInstanceMessages.get(instance),
            MessageType.PARTICIPANT_STATUS_CHANGE));
  }

  private boolean hasPendingMessage(Collection<Message> messages, MessageType type) {
    return messages != null && messages.stream()
        .anyMatch(message -> type.name().equals(message.getMsgType()));
  }

  private void recordClusterStatus(ClusterManagementMode mode, HelixDataAccessor accessor) {
    // update cluster status
    PropertyKey statusPropertyKey = accessor.keyBuilder().clusterStatus();
    ClusterStatus clusterStatus = accessor.getProperty(statusPropertyKey);
    if (clusterStatus == null) {
      clusterStatus = new ClusterStatus();
    }

    ClusterManagementMode.Type recordedType = clusterStatus.getManagementMode();
    ClusterManagementMode.Status recordedStatus = clusterStatus.getManagementModeStatus();

    // If there is any pending message sent by users, status could be computed as in progress.
    // Skip recording status change to avoid confusion after cluster is already fully frozen.
    if (ClusterManagementMode.Type.CLUSTER_PAUSE.equals(recordedType)
        && ClusterManagementMode.Status.COMPLETED.equals(recordedStatus)
        && ClusterManagementMode.Type.CLUSTER_PAUSE.equals(mode.getMode())
        && ClusterManagementMode.Status.IN_PROGRESS.equals(mode.getStatus())) {
      LOG.info("Skip recording status mode={}, status={}, because cluster is fully frozen",
          mode.getMode(), mode.getStatus());
      return;
    }

    if (!mode.getMode().equals(recordedType) || !mode.getStatus().equals(recordedStatus)) {
      // Only update status when it's different with metadata store
      clusterStatus.setManagementMode(mode.getMode());
      clusterStatus.setManagementModeStatus(mode.getStatus());
      if (!accessor.updateProperty(statusPropertyKey, clusterStatus)) {
        LOG.error("Failed to update cluster status {}", clusterStatus);
      }
    }
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

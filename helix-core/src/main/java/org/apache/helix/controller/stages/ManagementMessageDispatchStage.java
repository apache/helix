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

import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.model.Message;
import org.apache.helix.util.RebalanceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatches participant status change and pending state transition cancellation messages
 * for the management pipeline.
 */
public class ManagementMessageDispatchStage extends MessageDispatchStage {
  private static final Logger LOG = LoggerFactory.getLogger(ManagementMessageDispatchStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    MessageOutput messageOutput =
        event.getAttribute(AttributeName.MESSAGES_ALL.name());
    processEvent(event, messageOutput);

    // Send participant status change messages.
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    List<Message> messagesSent =
        super.sendMessages(manager.getHelixDataAccessor(), messageOutput.getStatusChangeMessages());
    ManagementControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());
    cache.cacheMessages(messagesSent);

    // Can exit management mode pipeline after fully in normal mode
    ClusterManagementMode managementMode = event.getAttribute(AttributeName.CLUSTER_STATUS.name());
    if (managementMode.isFullyInNormalMode()) {
      LogUtil.logInfo(LOG, _eventId,
          "Exiting management mode pipeline for cluster " + event.getClusterName());
      RebalanceUtil.enableManagementMode(event.getClusterName(), false);
    }
  }
}

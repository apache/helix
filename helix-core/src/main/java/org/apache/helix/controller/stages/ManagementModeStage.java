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

import org.apache.helix.controller.LogUtil;
import org.apache.helix.controller.dataproviders.ManagementControllerDataProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.RebalanceUtil;
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
    ManagementControllerDataProvider cache =
        event.getAttribute(AttributeName.ControllerDataProvider.name());

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
}

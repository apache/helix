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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerProperties;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.pipeline.StageException;
import org.apache.log4j.Logger;

/**
 * controller checks if participant version is compatible
 */
public class CompatibilityCheckStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(CompatibilityCheckStage.class.getName());

  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager manager = event.getAttribute("helixmanager");
    Cluster cluster = event.getAttribute("Cluster");
    if (manager == null || cluster == null) {
      throw new StageException("Missing attributes in event:" + event
          + ". Requires HelixManager | Cluster");
    }

    HelixManagerProperties properties = manager.getProperties();
    // Map<String, LiveInstance> liveInstanceMap = cache.getLiveInstances();
    Map<ParticipantId, Participant> liveParticipants = cluster.getLiveParticipantMap();
    for (Participant liveParticipant : liveParticipants.values()) {
      String participantVersion = liveParticipant.getLiveInstance().getHelixVersion();
      if (!properties.isParticipantCompatible(participantVersion)) {
        String errorMsg =
            "incompatible participant. pipeline will not continue. " + "controller: "
                + manager.getInstanceName() + ", controllerVersion: " + properties.getVersion()
                + ", minimumSupportedParticipantVersion: "
                + properties.getProperty("minimum_supported_version.participant")
                + ", participant: " + liveParticipant.getId() + ", participantVersion: "
                + participantVersion;
        LOG.error(errorMsg);
        throw new StageException(errorMsg);
      }
    }
  }
}

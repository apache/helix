package org.apache.helix.provisioning.yarn;

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

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Resource;
import org.apache.helix.api.State;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ContainerConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.ExternalView;

public class AppStatusReportGenerator {
  static String TAB = "\t";
  static String NEWLINE = "\n";

  String generateReport(HelixConnection connection, ClusterId clusterId) {
    if (!connection.isConnected()) {
      return "Unable to connect to cluster";
    }
    StringBuilder builder = new StringBuilder();
    ClusterAccessor clusterAccessor = connection.createClusterAccessor(clusterId);
    Cluster cluster = clusterAccessor.readCluster();
    Map<ParticipantId, Participant> participants = cluster.getParticipantMap();
    builder.append("AppName").append(TAB).append(clusterId).append(NEWLINE);
    Map<ResourceId, Resource> resources = cluster.getResourceMap();
    for (ResourceId resourceId : resources.keySet()) {
      builder.append("SERVICE").append(TAB).append(resourceId).append(NEWLINE);
      Resource resource = resources.get(resourceId);
      Map<ParticipantId, State> serviceStateMap = null;
      if (resource != null) {
        HelixDataAccessor accessor = connection.createDataAccessor(clusterId);
        ExternalView externalView =
            accessor.getProperty(accessor.keyBuilder().externalView(resourceId.stringify()));
        if (externalView != null) {
          serviceStateMap =
              externalView.getStateMap(PartitionId.from(resourceId.stringify() + "_0"));
        }
      }

      builder.append(TAB).append("CONTAINER_NAME").append(TAB).append(TAB)
          .append("CONTAINER_STATE").append(TAB).append("SERVICE_STATE").append(TAB)
          .append("CONTAINER_ID").append(NEWLINE);
      for (Participant participant : participants.values()) {
        // need a better check
        if (!participant.getId().stringify().startsWith(resource.getId().stringify())) {
          continue;
        }
        ContainerConfig containerConfig = participant.getContainerConfig();
        ContainerState containerState = ContainerState.UNDEFINED;
        ContainerId containerId = ContainerId.from("N/A");

        if (containerConfig != null) {
          containerId = containerConfig.getId();
          containerState = containerConfig.getState();
        }
        State participantState = null;
        if (serviceStateMap != null) {
          participantState = serviceStateMap.get(participant.getId());
        }
        if (participantState == null) {
          participantState = State.from("UNKNOWN");
        }
        builder.append(TAB).append(participant.getId()).append(TAB).append(containerState)
            .append(TAB).append(participantState).append(TAB).append(TAB).append(containerId);
        builder.append(NEWLINE);
      }

    }
    return builder.toString();

  }

  public static void main(String[] args) throws InterruptedException {
    AppStatusReportGenerator generator = new AppStatusReportGenerator();

    ZkHelixConnection connection = new ZkHelixConnection("localhost:2181");
    connection.connect();
    while (true) {
      String generateReport = generator.generateReport(connection, ClusterId.from("testApp1"));
      System.out.println(generateReport);
      Thread.sleep(10000);
      connection.createClusterManagementTool().addCluster("testApp1");
    }
    // connection.disconnect();
  }
}

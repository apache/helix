package org.apache.helix.provisioning.yarn;

import java.util.Map;

import jline.ConsoleReader;

import org.apache.helix.HelixConnection;
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
    Map<ParticipantId, Participant> participants = clusterAccessor.readParticipants();
    builder.append("AppName").append(TAB).append(clusterId).append(NEWLINE);
    Map<ResourceId, Resource> resources = clusterAccessor.readResources();
    for (ResourceId resourceId : resources.keySet()) {
      builder.append("SERVICE").append(TAB).append(resourceId).append(NEWLINE);
      Resource resource = resources.get(resourceId);
      Map<ParticipantId, State> serviceStateMap =
          resource.getExternalView().getStateMap(PartitionId.from(resourceId.stringify() + "_0"));

      builder.append(TAB).append("CONTAINER_NAME").append(TAB).append(TAB)
          .append("CONTAINER_STATE").append(TAB).append("SERVICE_STATE").append(TAB).append("CONTAINER_ID").append(NEWLINE);
      for (Participant participant : participants.values()) {
        // need a better check
        if (!participant.getId().stringify().startsWith(resource.getId().stringify())) {
          continue;
        }
        ContainerConfig containerConfig = participant.getContainerConfig();
        ContainerState containerState =ContainerState.UNDEFINED;
        ContainerId containerId = ContainerId.from("N/A");

        if (containerConfig != null) {
          containerId = containerConfig.getId();
          containerState = containerConfig.getState();
        }
        State participantState = serviceStateMap.get(participant.getId());
        if (participantState == null) {
          participantState = State.from("UNKNOWN");
        }
        builder.append(TAB).append(participant.getId()).append(TAB)
            .append(containerState).append(TAB).append(participantState).append(TAB).append(TAB).append(containerId);
        builder.append(NEWLINE);
      }

    }
    return builder.toString();

  }

  public static void main(String[] args) {
    AppStatusReportGenerator generator = new AppStatusReportGenerator();

    ZkHelixConnection connection = new ZkHelixConnection("localhost:2181");
    connection.connect();
    String generateReport = generator.generateReport(connection, ClusterId.from("testApp"));
    System.out.println(generateReport);
    connection.disconnect();
  }
}

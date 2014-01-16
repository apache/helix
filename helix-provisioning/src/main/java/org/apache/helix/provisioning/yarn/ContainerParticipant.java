package org.apache.helix.provisioning.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.helix.HelixConnection;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.manager.zk.AbstractParticipantService;

public class ContainerParticipant extends AbstractParticipantService {
  private static final Log LOG = LogFactory.getLog(ContainerParticipant.class);

  public ContainerParticipant(HelixConnection connection, ClusterId clusterId,
      ParticipantId participantId) {
    super(connection, clusterId, participantId);
  }

  @Override
  public void init() {
    // register a state model
  }

  @Override
  public void onPreJoinCluster() {
    // do tasks that require a connection
  }
}

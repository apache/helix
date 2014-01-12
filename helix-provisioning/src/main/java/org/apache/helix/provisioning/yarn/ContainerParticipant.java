package org.apache.helix.provisioning.yarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixParticipant;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;

import com.google.common.util.concurrent.AbstractService;

public class ContainerParticipant extends AbstractService {
  private static final Log LOG = LogFactory.getLog(ContainerParticipant.class);
  private final ClusterId _clusterId;
  private final ParticipantId _participantId;
  private HelixParticipant _participant;
  private HelixConnection _connection;

  public ContainerParticipant(HelixConnection connection, ClusterId clusterId,
      ParticipantId participantId) {
    _connection = connection;
    _clusterId = clusterId;
    _participantId = participantId;
  }

  @Override
  protected void doStart() {
    _participant = _connection.createParticipant(_clusterId, _participantId);
    // register statemachine
    _participant.startAsync();
    notifyStarted();
  }

  @Override
  protected void doStop() {
    _participant.stopAsync();
    notifyStopped();
  }
}

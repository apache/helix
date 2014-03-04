package org.apache.helix.provisioning.yarn.example;

import org.apache.helix.HelixConnection;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.participant.AbstractParticipantService;
import org.apache.helix.provisioning.ServiceConfig;
import org.apache.helix.provisioning.participant.StatelessParticipantService;
import org.apache.log4j.Logger;

public class HelloWorldService extends StatelessParticipantService {

  private static Logger LOG = Logger.getLogger(AbstractParticipantService.class);

  static String SERVICE_NAME = "HelloWorld";

  public HelloWorldService(HelixConnection connection, ClusterId clusterId,
      ParticipantId participantId) {
    super(connection, clusterId, participantId, SERVICE_NAME);
  }

  @Override
  protected void init(ServiceConfig serviceConfig) {
    LOG.info("Initialized service with config " + serviceConfig);
  }

  @Override
  protected void goOnline() {
    LOG.info("HelloWorld service is told to go online");
  }

  @Override
  protected void goOffine() {
    LOG.info("HelloWorld service is told to go offline");
  }

}

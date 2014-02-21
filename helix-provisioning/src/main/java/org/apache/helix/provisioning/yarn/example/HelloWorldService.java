package org.apache.helix.provisioning.yarn.example;

import org.apache.helix.HelixConnection;
import org.apache.helix.api.accessor.ResourceAccessor;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.AbstractParticipantService;
import org.apache.log4j.Logger;

public class HelloWorldService extends AbstractParticipantService {

  private static Logger LOG = Logger.getLogger(AbstractParticipantService.class);

  static String SERVICE_NAME = "HelloWorld";

  public HelloWorldService(HelixConnection connection, ClusterId clusterId,
      ParticipantId participantId) {
    super(connection, clusterId, participantId);
  }

  /**
   * init method to setup appropriate call back handlers.
   */
  @Override
  public void init() {
    ClusterId clusterId = getClusterId();
    ResourceAccessor resourceAccessor = getConnection().createResourceAccessor(clusterId);
    UserConfig serviceConfig = resourceAccessor.readUserConfig(ResourceId.from(SERVICE_NAME));
    LOG.info("Starting service:" + SERVICE_NAME + " with configuration:" + serviceConfig);

    HelloWorldStateModelFactory stateModelFactory = new HelloWorldStateModelFactory();
    getParticipant().getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from("StatelessService"), stateModelFactory);

  }

}

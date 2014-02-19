package org.apache.helix.provisioning.yarn.example;

import org.apache.helix.HelixConnection;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.AbstractParticipantService;


public class HelloWorldService extends AbstractParticipantService {

	public HelloWorldService(HelixConnection connection, ClusterId clusterId,
			ParticipantId participantId) {
		super(connection, clusterId, participantId);
	}
	
	@Override
	public void init() {
		HelloWorldStateModelFactory stateModelFactory = new HelloWorldStateModelFactory();
		getParticipant().getStateMachineEngine().registerStateModelFactory(StateModelDefId.from("StatelessService"), stateModelFactory);
	}

  @Override
  public void onPreJoinCluster() {
    //this will be invoked prior to 
  }
}


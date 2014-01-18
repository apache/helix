package org.apache.helix.provisioning.yarn.example;

import org.apache.helix.api.id.PartitionId;
import org.apache.helix.participant.statemachine.HelixStateModelFactory;
import org.apache.helix.participant.statemachine.StateModel;

public class HelloWorldStateModelFactory extends HelixStateModelFactory<StateModel> {
	@Override
	public StateModel createNewStateModel(PartitionId partitionId) {
		return new HelloWorldStateModel(partitionId);
	}
}

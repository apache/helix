package org.apache.helix.provisioning.yarn.example;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

@StateModelInfo(initialState = "OFFLINE", states = { "OFFLINE", "ONLINE",
		"ERROR" })
public class HelloWorldStateModel extends StateModel {

	public HelloWorldStateModel(PartitionId partitionId) {
		// TODO Auto-generated constructor stub
	}

	@Transition(to = "ONLINE", from = "OFFLINE")
	public void onBecomeOnlineFromOffline(Message message,
			NotificationContext context) throws Exception {
		System.out.println("Started HelloWorld service");
	}

	@Transition(to = "OFFLINE", from = "ONLINE")
	public void onBecomeOfflineFromOnline(Message message,
			NotificationContext context) throws InterruptedException {
		System.out.println("Stopped HelloWorld service");
	}
}

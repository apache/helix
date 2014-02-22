package org.apache.helix.provisioning.yarn.example;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(initialState = "OFFLINE", states = {
    "OFFLINE", "ONLINE", "ERROR"
})
public class HelloWorldStateModel extends StateModel {

  private static Logger LOG = Logger.getLogger(HelloWorldStateModel.class);

  public HelloWorldStateModel(PartitionId partitionId) {
    // ignore the partitionId
  }

  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context)
      throws Exception {
    LOG.info("Started HelloWorld service");
  }

  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Stopped HelloWorld service");
  }
}

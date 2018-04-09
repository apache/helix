package org.apache.helix.examples;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerResourceOnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerResourceOnlineOfflineStateModelFactory.class);

  public BrokerResourceOnlineOfflineStateModelFactory() {

  }

  public static String getStateModelDef() {
    return "BrokerResourceOnlineOfflineStateModel";
  }

  public StateModel createNewStateModel(String resourceName) {
    return new BrokerResourceOnlineOfflineStateModelFactory.BrokerResourceOnlineOfflineStateModel();
  }

  @StateModelInfo(
      states = {"{'OFFLINE','ONLINE', 'DROPPED'}"},
      initialState = "OFFLINE"
  )
  public class BrokerResourceOnlineOfflineStateModel extends StateModel {
    public BrokerResourceOnlineOfflineStateModel() {
    }

    @Transition(
        from = "OFFLINE",
        to = "ONLINE"
    )
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
        LOGGER.info("Become Online from Offline");
    }

    @Transition(
        from = "ONLINE",
        to = "OFFLINE"
    )
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOGGER.info("Become Offline from Online");

    }

    @Transition(
        from = "OFFLINE",
        to = "DROPPED"
    )
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("Become Dropped from Offline");
    }

    @Transition(
        from = "ONLINE",
        to = "DROPPED"
    )
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      LOGGER.info("Become Dropped from Online");
    }

    @Transition(
        from = "ERROR",
        to = "OFFLINE"
    )
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      LOGGER.info("Become Offline from Error");
    }
  }
}


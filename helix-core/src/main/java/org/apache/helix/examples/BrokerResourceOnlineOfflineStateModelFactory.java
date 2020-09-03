package org.apache.helix.examples;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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


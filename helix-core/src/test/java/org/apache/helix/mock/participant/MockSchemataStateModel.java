package org.apache.helix.mock.participant;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

// mock STORAGE_DEFAULT_SM_SCHEMATA state model
@StateModelInfo(initialState = "OFFLINE", states = {
    "MASTER", "DROPPED", "ERROR"
})
public class MockSchemataStateModel extends TransitionHandler {
  private static Logger LOG = Logger.getLogger(MockSchemataStateModel.class);

  @Transition(to = "MASTER", from = "OFFLINE")
  public void onBecomeMasterFromOffline(Message message, NotificationContext context) {
    LOG.info("Become MASTER from OFFLINE");
  }

  @Transition(to = "OFFLINE", from = "MASTER")
  public void onBecomeOfflineFromMaster(Message message, NotificationContext context) {
    LOG.info("Become OFFLINE from MASTER");
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    LOG.info("Become DROPPED from OFFLINE");
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    LOG.info("Become OFFLINE from ERROR");
  }
}

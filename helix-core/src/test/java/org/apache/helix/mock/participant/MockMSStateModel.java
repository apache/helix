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

// mock master-slave state model
@StateModelInfo(initialState = "OFFLINE", states = {
    "MASTER", "SLAVE", "ERROR"
})
public class MockMSStateModel extends TransitionHandler {
  private static Logger LOG = Logger.getLogger(MockMSStateModel.class);

  protected MockTransition _transition;

  public MockMSStateModel(MockTransition transition) {
    _transition = transition;
  }

  public void setTransition(MockTransition transition) {
    _transition = transition;
  }

  // overwrite default error->dropped transition
  @Override
  @Transition(to = "DROPPED", from = "ERROR")
  public void onBecomeDroppedFromError(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become DROPPED from ERROR");
    if (_transition != null) {
      _transition.doTransition(message, context);
    }
  }

  @Transition(to = "SLAVE", from = "OFFLINE")
  public void onBecomeSlaveFromOffline(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become SLAVE from OFFLINE");
    if (_transition != null) {
      _transition.doTransition(message, context);

    }
  }

  @Transition(to = "MASTER", from = "SLAVE")
  public void onBecomeMasterFromSlave(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become MASTER from SLAVE");
    if (_transition != null) {
      _transition.doTransition(message, context);
    }
  }

  @Transition(to = "SLAVE", from = "MASTER")
  public void onBecomeSlaveFromMaster(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become SLAVE from MASTER");
    if (_transition != null) {
      _transition.doTransition(message, context);
    }
  }

  @Transition(to = "OFFLINE", from = "SLAVE")
  public void onBecomeOfflineFromSlave(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become OFFLINE from SLAVE");
    if (_transition != null) {
      _transition.doTransition(message, context);
    }
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become DROPPED from OFFLINE");
    if (_transition != null) {
      _transition.doTransition(message, context);
    }
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become OFFLINE from ERROR");
    // System.err.println("Become OFFLINE from ERROR");
    if (_transition != null) {
      _transition.doTransition(message, context);
    }
  }

  @Transition(to = "LEADER", from = "MASTER")
  public void onBecomeLeaderFromMaster(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become LEADER from MASTER");
  }

  @Transition(to = "MASTER", from = "LEADER")
  public void onBecomeMasterFromLeader(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.info("Become MASTER from LEADER");
  }

  @Override
  public void reset() {
    LOG.info("Default MockMSStateModel.reset() invoked");
    if (_transition != null) {
      _transition.doReset();
    }
  }
}

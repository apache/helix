package org.apache.helix.mock.participant;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
// mock delay master-slave state model
@StateModelInfo(initialState = "OFFLINE", states = {
    "MASTER", "SLAVE", "ERROR"
})
public class MockDelayMSStateModel extends StateModel {
  private static Logger LOG = LoggerFactory.getLogger(MockDelayMSStateModel.class);
  private long _delay;

  public MockDelayMSStateModel(long delay) {
    _delay = delay;
    _cancelled = false;
  }

  @Transition(to = "SLAVE", from = "OFFLINE")
  public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
    if (_delay > 0) {
      try {
        Thread.sleep(_delay);
      } catch (InterruptedException e) {
        LOG.error("Failed to sleep for " + _delay);
      }
    }
    LOG.info("Become SLAVE from OFFLINE");
  }

  @Transition(to = "MASTER", from = "SLAVE")
  public void onBecomeMasterFromSlave(Message message, NotificationContext context)
      throws InterruptedException {
    if (_delay < 0) {
        Thread.sleep(Math.abs(_delay));
    }
    LOG.error("Become MASTER from SLAVE");
  }

  @Transition(to = "OFFLINE", from = "SLAVE")
  public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
    LOG.info("Become OFFLINE from SLAVE");
  }
}

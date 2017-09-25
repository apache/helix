package org.apache.helix.mock.participant;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

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
  private static Logger LOG = Logger.getLogger(MockDelayMSStateModel.class);

  private long _delay;

  public MockDelayMSStateModel(long delay) {
    _delay = delay;
  }

  @Transition(to = "SLAVE", from = "OFFLINE")
  public void onBecomeSLAVEFromOffline(Message message, NotificationContext context) {
    if (_delay > 0) {
      try {
        Thread.sleep(_delay);
      } catch (InterruptedException e) {
        LOG.error("Failed to sleep for " + _delay);
      }
    }
    LOG.info("Become SLAVE from OFFLINE");
  }

  @Transition(to = "ONLINE", from = "SLAVE")
  public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
    LOG.info("Become ONLINE from SLAVE");
  }


}

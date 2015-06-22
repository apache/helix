package org.apache.helix.recipes.rabbitmq;

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

import org.apache.log4j.Logger;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

@StateModelInfo(initialState = "OFFLINE", states = {
    "ONLINE", "ERROR"
})
public class ConsumerTransitionHandler extends TransitionHandler {
  private static Logger LOG = Logger.getLogger(ConsumerTransitionHandler.class);

  private final ParticipantId _consumerId;
  private final PartitionId _partition;

  private final String _mqServer;
  private ConsumerThread _thread = null;

  public ConsumerTransitionHandler(ParticipantId consumerId, PartitionId partition, String mqServer) {
    _partition = partition;
    _consumerId = consumerId;
    _mqServer = mqServer;
  }

  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    LOG.debug(_consumerId + " becomes ONLINE from OFFLINE for " + _partition);

    if (_thread == null) {
      LOG.debug("Starting ConsumerThread for " + _partition + "...");
      _thread = new ConsumerThread(_partition, _mqServer, _consumerId);
      _thread.start();
      LOG.debug("Starting ConsumerThread for " + _partition + " done");

    }
  }

  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.debug(_consumerId + " becomes OFFLINE from ONLINE for " + _partition);

    if (_thread != null) {
      LOG.debug("Stopping " + _consumerId + " for " + _partition + "...");

      _thread.interrupt();
      _thread.join(2000);
      _thread = null;
      LOG.debug("Stopping " + _consumerId + " for " + _partition + " done");

    }
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    LOG.debug(_consumerId + " becomes DROPPED from OFFLINE for " + _partition);
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    LOG.debug(_consumerId + " becomes OFFLINE from ERROR for " + _partition);
  }

  @Override
  public void reset() {
    LOG.warn("Default reset() invoked");

    if (_thread != null) {
      LOG.debug("Stopping " + _consumerId + " for " + _partition + "...");

      _thread.interrupt();
      try {
        _thread.join(2000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      _thread = null;
      LOG.debug("Stopping " + _consumerId + " for " + _partition + " done");

    }
  }
}

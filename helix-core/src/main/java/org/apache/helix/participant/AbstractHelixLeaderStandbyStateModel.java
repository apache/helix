package org.apache.helix.participant;

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
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic leader-standby state model impl for helix services. It requires implementing
 * service-specific o->s, s->l, l->s, s->o, and reset methods, and provides
 * default impl for the reset
 */

@StateModelInfo(initialState = "OFFLINE", states = {
    "LEADER", "STANDBY"
})
public abstract class AbstractHelixLeaderStandbyStateModel extends StateModel {
  private final static Logger logger =
      LoggerFactory.getLogger(AbstractHelixLeaderStandbyStateModel.class);
  protected final String _zkAddr;

  public AbstractHelixLeaderStandbyStateModel(String zkAddr) {
    _zkAddr = zkAddr;
    StateModelParser parser = new StateModelParser();
    _currentState = parser.getInitialState(getClass());
  }

  @Transition(to = "STANDBY", from = "OFFLINE")
  public abstract void onBecomeStandbyFromOffline(Message message, NotificationContext context);

  @Transition(to = "LEADER", from = "STANDBY")
  public abstract void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception;

  @Transition(to = "STANDBY", from = "LEADER")
  public abstract void onBecomeStandbyFromLeader(Message message, NotificationContext context);

  @Transition(to = "OFFLINE", from = "STANDBY")
  public abstract void onBecomeOfflineFromStandby(Message message, NotificationContext context);

  @Transition(to = "DROPPED", from = "OFFLINE")
  public abstract void onBecomeDroppedFromOffline(Message message, NotificationContext context);

  @Transition(to = "OFFLINE", from = "DROPPED")
  public void onBecomeOfflineFromDropped(
      Message message, NotificationContext context) {
    reset();
    logStateTransition("DROPPED", "OFFLINE", message == null ? "" : message.getPartitionName(),
        message == null ? "" : message.getTgtName());
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    reset();
    logStateTransition("ERROR", "OFFLINE", message == null ? "" : message.getPartitionName(),
        message == null ? "" : message.getTgtName());
  }

  @Override
  public void rollbackOnError(Message message, NotificationContext context,
      StateTransitionError error) {
    reset();
    logger.info("{} rolled back on error. Code: {}, Exception: {}",
        getStateModeInstanceDescription(message == null ? "" : message.getPartitionName(),
            message == null ? "" : message.getTgtName()), error == null ? "" : error.getCode(),
        error == null ? "" : error.getException());
  }

  @Override
  public abstract void reset();

  protected String getStateModeInstanceDescription(String partitionName, String instanceName) {
    return String.format("%s(%s) for partition %s on instance %s",
        this.getClass().getSimpleName(), this.hashCode(), partitionName, instanceName);
  }

  protected void logStateTransition(String fromState, String toState, String partitionName,
      String instanceName) {
    logger.info("Helix Service {} became {} from {}.",
        getStateModeInstanceDescription(partitionName, instanceName), toState, fromState);
  }
}

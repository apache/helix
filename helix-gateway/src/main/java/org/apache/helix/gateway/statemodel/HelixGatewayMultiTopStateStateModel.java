package org.apache.helix.gateway.statemodel;

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
import org.apache.helix.gateway.participant.HelixGatewayParticipant;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = "OFFLINE", states = {})
public class HelixGatewayMultiTopStateStateModel extends StateModel {
  private static final Logger _logger =
      LoggerFactory.getLogger(HelixGatewayMultiTopStateStateModel.class);

  private final HelixGatewayParticipant _helixGatewayParticipant;

  public HelixGatewayMultiTopStateStateModel(
      HelixGatewayParticipant helixGatewayParticipant) {
    _helixGatewayParticipant = helixGatewayParticipant;
  }

  @Transition(to = "*", from = "*")
  public void genericStateTransitionHandler(Message message, NotificationContext context)
      throws Exception {
    _helixGatewayParticipant.processStateTransitionMessage(message);
  }

  @Override
  public void reset() {
    // no-op we don't want to start from init state again.
  }

  @Override
  public void rollbackOnError(Message message, NotificationContext context,
      StateTransitionError error) {
    _helixGatewayParticipant.handleStateTransitionError(message, error);
  }
}
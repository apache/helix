package org.apache.helix.api;

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
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

public abstract class TransitionHandler {
  public static final String DEFAULT_INITIAL_STATE = "OFFLINE";
  Logger logger = Logger.getLogger(TransitionHandler.class);

  // TODO Get default state from implementation or from state model annotation
  // StateModel with initial state other than OFFLINE should override this field
  protected String _currentState = DEFAULT_INITIAL_STATE;

  /**
   * requested-state is used (e.g. by task-framework) to request next state
   */
  protected String _requestedState = null;

  public String getCurrentState() {
    return _currentState;
  }

  // @transition(from='from', to='to')
  public void defaultTransitionHandler() {
    logger
        .error("Default default handler. The idea is to invoke this if no transition method is found. Yet to be implemented");
  }

  public boolean updateState(String newState) {
    _currentState = newState;
    return true;
  }

  /**
   * Get requested-state
   * @return requested-state
   */
  public String getRequestedState() {
    return _requestedState;
  }

  /**
   * Set requested-state
   * @param requestedState
   */
  public void setRequestedState(String requestedState) {
    _requestedState = requestedState;
  }

  /**
   * Called when error occurs in state transition
   * TODO:enforce subclass to write this
   * @param message
   * @param context
   * @param error
   */
  public void rollbackOnError(Message message, NotificationContext context,
      StateTransitionError error) {

    logger.error("Default rollback method invoked on error. Error Code: " + error.getCode());

  }

  /**
   * Called when the state model is reset
   */
  public void reset() {
    logger
        .warn("Default reset method invoked. Either because the process longer own this resource or session timedout");
  }

  /**
   * default transition for drop partition in error state
   * @param message
   * @param context
   * @throws InterruptedException
   */
  @Transition(to = "DROPPED", from = "ERROR")
  public void onBecomeDroppedFromError(Message message, NotificationContext context)
      throws Exception {
    logger.info("Default ERROR->DROPPED transition invoked.");
  }

}

package org.apache.helix.participant.statemachine;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StateModel {
  static final String DEFAULT_INITIAL_STATE = "OFFLINE";
  protected boolean _cancelled;
  Logger logger = LoggerFactory.getLogger(StateModel.class);

  // TODO Get default state from implementation or from state model annotation
  // StateModel with initial state other than OFFLINE should override this field
  protected String _currentState = DEFAULT_INITIAL_STATE;

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

  /**
   * Default implementation for cancelling state transition
   *
   * IMPORTANT:
   *
   * 1. Be careful with the implementation of this method. There is no
   * grantee that this method is called before user state transition method invoked.
   * Please make sure the implemention contains logic for checking state transition already started.
   * Similar to this situation, when this cancel method has been called. Helix does not grantee the
   * state transition is still running. The state transition could be completed.
   *
   * 2. This cancel method should not throw HelixRollbackException. It is better to trigger the real
   * state transition to throw HelixRollbackException if user would like to cancel the current
   * running state transition.
   */
  public void cancel() {
    _cancelled = true;
  }

  /**
   * Default implementation to check whether state transition has been cancelled or not
   * @return
   */
  public boolean isCancelled() {
    return _cancelled;
  }
}

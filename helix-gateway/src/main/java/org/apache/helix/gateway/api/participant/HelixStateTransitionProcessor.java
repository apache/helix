package org.apache.helix.gateway.api.participant;

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

import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateTransitionError;

/**
 * Process state transition message.
 */
public interface HelixStateTransitionProcessor {
  /**
   * Process state transition message.
   * @param message state transition message
   * @throws Exception if failed to process the message
   */
  void processStateTransitionMessage(Message message) throws Exception;

  /**
   * Handle state transition error. This results from state transition handler throwing an exception or
   * timing out.
   *
   * @param message state transition message
   * @param error state transition error
   */
  void handleStateTransitionError(Message message, StateTransitionError error);
}

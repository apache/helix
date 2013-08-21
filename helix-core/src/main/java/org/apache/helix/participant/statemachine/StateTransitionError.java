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

import org.apache.helix.messaging.handling.MessageHandler.ErrorCode;
import org.apache.helix.messaging.handling.MessageHandler.ErrorType;

public class StateTransitionError {
  private final Exception _exception;
  private final ErrorCode _code;
  private final ErrorType _type;

  public StateTransitionError(ErrorType type, ErrorCode code, Exception e) {
    _type = type;
    _code = code;
    _exception = e;
  }

  public Exception getException() {
    return _exception;
  }

  public ErrorCode getCode() {
    return _code;
  }
}

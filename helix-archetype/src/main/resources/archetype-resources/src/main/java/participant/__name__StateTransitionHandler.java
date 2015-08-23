package ${package}.participant;

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

@StateModelInfo(states = "{'OFFLINE','ONLINE'}", initialState = "OFFLINE")
public class ${name}StateTransitionHandler extends TransitionHandler {
  @Transition(from = "OFFLINE", to = "ONLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Application logic to handle transition                                                     //
    // For example, you might start a service, run initialization, etc                            //
    ////////////////////////////////////////////////////////////////////////////////////////////////
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Application logic to handle transition                                                     //
    // For example, you might shutdown a service, log this event, or change monitoring settings   //
    ////////////////////////////////////////////////////////////////////////////////////////////////
  }
}

package org.apache.helix.lockmanager;

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

@StateModelInfo(initialState = "OFFLINE", states = {
    "OFFLINE", "ONLINE"
})
public class Lock extends TransitionHandler {
  private String lockName;

  public Lock(String lockName) {
    this.lockName = lockName;
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void lock(Message m, NotificationContext context) {
    System.out.println(context.getManager().getInstanceName() + " acquired lock:" + lockName);
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void release(Message m, NotificationContext context) {
    System.out.println(context.getManager().getInstanceName() + " releasing lock:" + lockName);
  }

}

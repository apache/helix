package org.apache.helix.examples;

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
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.Message;

public class OnlineOfflineStateModelFactory extends StateTransitionHandlerFactory<TransitionHandler> {
  int _delay;

  public OnlineOfflineStateModelFactory(int delay) {
    _delay = delay;
  }

  @Override
  public TransitionHandler createStateTransitionHandler(ResourceId resource, PartitionId stateUnitKey) {
    OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel();
    stateModel.setDelay(_delay);
    return stateModel;
  }

  public static class OnlineOfflineStateModel extends TransitionHandler {
    int _transDelay = 0;

    public void setDelay(int delay) {
      _transDelay = delay > 0 ? delay : 0;
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      System.out.println("OnlineOfflineStateModel.onBecomeOnlineFromOffline()");
      sleep();
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      System.out.println("OnlineOfflineStateModel.onBecomeOfflineFromOnline()");
      sleep();
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      System.out.println("OnlineOfflineStateModel.onBecomeDroppedFromOffline()");
      sleep();

    }

    private void sleep() {
      try {
        Thread.sleep(_transDelay);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

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
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class OnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  int _delay;
  String _instanceName = "";

  public OnlineOfflineStateModelFactory(int delay) {
    _delay = delay;
  }

  public OnlineOfflineStateModelFactory(String instanceName) {
    _instanceName = instanceName;
    _delay = 10;
  }

  public OnlineOfflineStateModelFactory(String instanceName, int delay) {
    _instanceName = instanceName;
    _delay = delay;
  }

  public OnlineOfflineStateModelFactory() {
    this(10);
  }

  @Override
  public StateModel createNewStateModel(String resourceName, String stateUnitKey) {
    OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel();
    stateModel.setDelay(_delay);
    stateModel.setInstanceName(_instanceName);
    return stateModel;
  }

  public static class OnlineOfflineStateModel extends StateModel {
    int _transDelay = 0;
    String _instanceName = "";

    public void setDelay(int delay) {
      _transDelay = delay > 0 ? delay : 0;
    }

    public void setInstanceName(String instanceName) {
      _instanceName = instanceName;
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      System.out.println(
          "OnlineOfflineStateModelFactory.onBecomeOnlineFromOffline():" + _instanceName
              + " transitioning from " + message.getFromState() + " to " + message.getToState()
              + " for " + message.getResourceName() + " " + message.getPartitionName());
      sleep();
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      System.out.println(
          "OnlineOfflineStateModelFactory.onBecomeOfflineFromOnline():" + _instanceName
              + " transitioning from " + message.getFromState() + " to " + message.getToState()
              + " for " + message.getResourceName() + " " + message.getPartitionName());
      sleep();
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      System.out.println(
          "OnlineOfflineStateModelFactory.onBecomeDroppedFromOffline():" + _instanceName
              + " transitioning from " + message.getFromState() + " to " + message.getToState()
              + " for " + message.getResourceName() + " " + message.getPartitionName());
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

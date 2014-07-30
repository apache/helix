package org.apache.helix.manager;

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

import java.util.List;

import org.apache.helix.ControllerChangeListener;
import org.apache.helix.CurrentStateChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.MessageListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;

public class MockListener implements IdealStateChangeListener, LiveInstanceChangeListener,
    InstanceConfigChangeListener, CurrentStateChangeListener, ExternalViewChangeListener,
    ControllerChangeListener, MessageListener

{
  public boolean isIdealStateChangeListenerInvoked = false;
  public boolean isLiveInstanceChangeListenerInvoked = false;
  public boolean isCurrentStateChangeListenerInvoked = false;
  public boolean isMessageListenerInvoked = false;
  public boolean isConfigChangeListenerInvoked = false;
  public boolean isExternalViewChangeListenerInvoked = false;
  public boolean isControllerChangeListenerInvoked = false;

  public void reset() {
    isIdealStateChangeListenerInvoked = false;
    isLiveInstanceChangeListenerInvoked = false;
    isCurrentStateChangeListenerInvoked = false;
    isMessageListenerInvoked = false;
    isConfigChangeListenerInvoked = false;
    isExternalViewChangeListenerInvoked = false;
    isControllerChangeListenerInvoked = false;
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext) {
    isIdealStateChangeListenerInvoked = true;
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    isLiveInstanceChangeListenerInvoked = true;
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    isConfigChangeListenerInvoked = true;
  }

  @Override
  public void onStateChange(String instanceName, List<CurrentState> statesInfo,
      NotificationContext changeContext) {
    isCurrentStateChangeListenerInvoked = true;
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    isExternalViewChangeListenerInvoked = true;
  }

  @Override
  public void onControllerChange(NotificationContext changeContext) {
    isControllerChangeListenerInvoked = true;
  }

  @Override
  public void onMessage(String instanceName, List<Message> messages,
      NotificationContext changeContext) {
    isMessageListenerInvoked = true;
  }
}

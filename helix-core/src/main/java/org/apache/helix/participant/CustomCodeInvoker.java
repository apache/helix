package org.apache.helix.participant;

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

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.log4j.Logger;

public class CustomCodeInvoker implements LiveInstanceChangeListener, InstanceConfigChangeListener,
    ExternalViewChangeListener {
  private static Logger LOG = Logger.getLogger(CustomCodeInvoker.class);
  private final CustomCodeCallbackHandler _callback;
  private final PartitionId _partitionKey;

  public CustomCodeInvoker(CustomCodeCallbackHandler callback, PartitionId partitionKey) {
    _callback = callback;
    _partitionKey = partitionKey;
  }

  private void callParticipantCode(NotificationContext context) {
    // since ZkClient.unsubscribe() does not immediately remove listeners
    // from zk, it is possible that two listeners exist when leadership transfers
    // therefore, double check to make sure only one participant invokes the code
    if (context.getType() == Type.CALLBACK) {
      HelixManager manager = context.getManager();
      // DataAccessor accessor = manager.getDataAccessor();
      HelixDataAccessor accessor = manager.getHelixDataAccessor();
      Builder keyBuilder = accessor.keyBuilder();

      String instance = manager.getInstanceName();
      String sessionId = manager.getSessionId();

      // get resource name from partition key: "PARTICIPANT_LEADER_XXX_0"
      String resourceName = _partitionKey.stringify().substring(0, _partitionKey.stringify().lastIndexOf('_'));

      CurrentState curState =
          accessor.getProperty(keyBuilder.currentState(instance, sessionId, resourceName));
      if (curState == null) {
        return;
      }

      String state = curState.getState(_partitionKey.stringify());
      if (state == null || !state.equalsIgnoreCase("LEADER")) {
        return;
      }
    }

    try {
      _callback.onCallback(context);
    } catch (Exception e) {
      LOG.error("Error invoking callback:" + _callback, e);
    }
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    LOG.info("onLiveInstanceChange() invoked");
    callParticipantCode(changeContext);
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    LOG.info("onConfigChange() invoked");
    callParticipantCode(changeContext);
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    LOG.info("onExternalViewChange() invoked");
    callParticipantCode(changeContext);
  }

}

/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.participant;

import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.ExternalView;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.model.LiveInstance;

public class CustomCodeInvoker implements LiveInstanceChangeListener, ConfigChangeListener,
    ExternalViewChangeListener
{
  private static Logger LOG = Logger.getLogger(CustomCodeInvoker.class);
  private final CustomCodeCallbackHandler _callback;
  private final String _partitionKey;

  public CustomCodeInvoker(CustomCodeCallbackHandler callback, String partitionKey)
  {
    _callback = callback;
    _partitionKey = partitionKey;
  }

  private void callParticipantCode(NotificationContext context)
  {
//    System.out.println("callback invoked. type:" + context.getType().toString());
    if (context.getType() == Type.INIT || context.getType() == Type.CALLBACK)
    {
      // since ZkClient.unsubscribe() does not immediately remove listeners
      // from zk, it is possible that two listeners exist when leadership transfers
      // therefore, double check to make sure only one participant invokes the code
      if (context.getType() == Type.CALLBACK)
      {
        HelixManager manager = context.getManager();
//        DataAccessor accessor = manager.getDataAccessor();
        HelixDataAccessor accessor = manager.getHelixDataAccessor();
        Builder keyBuilder = accessor.keyBuilder();
        
        String instance = manager.getInstanceName();
        String sessionId = manager.getSessionId();
        
        // get resource name from partition key: "PARTICIPANT_LEADER_XXX_0"
        String resourceName = _partitionKey.substring(0, _partitionKey.lastIndexOf('_'));
        
        CurrentState curState = accessor.getProperty(keyBuilder.currentState(instance, sessionId, resourceName));
        if (curState == null)
        {
          return;
        }
          
        String state = curState.getState(_partitionKey);
        if (state == null || !state.equalsIgnoreCase("LEADER"))
        {
          return;
        }
      }

      try
      {
        _callback.onCallback(context);
      } 
      catch (Exception e)
      {
        LOG.error("Error invoking callback:"+ _callback,e);
      }
    }
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext)
  {
    LOG.info("onLiveInstanceChange() invoked");
    callParticipantCode(changeContext);
  }

  @Override
  public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext)
  {
    LOG.info("onConfigChange() invoked");
    callParticipantCode(changeContext);
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext)
  {
    LOG.info("onExternalViewChange() invoked");
    callParticipantCode(changeContext);
  }

}

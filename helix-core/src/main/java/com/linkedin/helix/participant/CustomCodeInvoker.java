package com.linkedin.helix.participant;

import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.NotificationContext.Type;
import com.linkedin.helix.PropertyType;
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
        DataAccessor accessor = manager.getDataAccessor();
        String instance = manager.getInstanceName();
        String sessionId = manager.getSessionId();
        
        // get resource group name from partition key: "PARTICIPANT_LEADER_XXX_0"
        String resGroupName = _partitionKey.substring(0, _partitionKey.lastIndexOf('_'));
        CurrentState curState = accessor.getProperty(CurrentState.class,
            PropertyType.CURRENTSTATES, instance, sessionId, resGroupName);
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

package com.linkedin.clustermanager.agent;

import java.util.List;

import com.linkedin.clustermanager.ConfigChangeListener;
import com.linkedin.clustermanager.ControllerChangeListener;
import com.linkedin.clustermanager.CurrentStateChangeListener;
import com.linkedin.clustermanager.ExternalViewChangeListener;
import com.linkedin.clustermanager.IdealStateChangeListener;
import com.linkedin.clustermanager.LiveInstanceChangeListener;
import com.linkedin.clustermanager.MessageListener;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.CurrentState;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.Message;

public class MockListener implements IdealStateChangeListener, LiveInstanceChangeListener,
    ConfigChangeListener, CurrentStateChangeListener, ExternalViewChangeListener,
    ControllerChangeListener, MessageListener

{
  public boolean isIdealStateChangeListenerInvoked = false;
  public boolean isLiveInstanceChangeListenerInvoked = false;
  public boolean isCurrentStateChangeListenerInvoked = false;
  public boolean isMessageListenerInvoked = false;
  public boolean isConfigChangeListenerInvoked = false;
  public boolean isExternalViewChangeListenerInvoked = false;
  public boolean isControllerChangeListenerInvoked = false;

  public void reset()
  {
    isIdealStateChangeListenerInvoked = false;
    isLiveInstanceChangeListenerInvoked = false;
    isCurrentStateChangeListenerInvoked = false;
    isMessageListenerInvoked = false;
    isConfigChangeListenerInvoked = false;
    isExternalViewChangeListenerInvoked = false;
    isControllerChangeListenerInvoked = false;
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealState, NotificationContext changeContext)
  {
    isIdealStateChangeListenerInvoked = true;
  }

  @Override
  public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext)
  {
    isLiveInstanceChangeListenerInvoked = true;
  }

  @Override
  public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext)
  {
    isConfigChangeListenerInvoked = true;
  }

  @Override
  public void onStateChange(String instanceName,
                            List<CurrentState> statesInfo,
                            NotificationContext changeContext)
  {
    isCurrentStateChangeListenerInvoked = true;
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
                                   NotificationContext changeContext)
  {
    isExternalViewChangeListenerInvoked = true;
  }

  @Override
  public void onControllerChange(NotificationContext changeContext)
  {
    isControllerChangeListenerInvoked = true;
  }

  @Override
  public void onMessage(String instanceName,
                        List<Message> messages,
                        NotificationContext changeContext)
  {
    isMessageListenerInvoked = true;
  }
}

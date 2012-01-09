package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.model.CurrentState;

public interface CurrentStateChangeListener
{

  public void onStateChange(String instanceName, List<CurrentState> statesInfo,
      NotificationContext changeContext);

}

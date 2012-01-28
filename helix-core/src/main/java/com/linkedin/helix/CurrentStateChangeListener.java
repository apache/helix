package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.model.CurrentState;

public interface CurrentStateChangeListener
{

  public void onStateChange(String instanceName, List<CurrentState> statesInfo,
      NotificationContext changeContext);

}

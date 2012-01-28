package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.model.LiveInstance;

public interface LiveInstanceChangeListener
{

  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext);

}

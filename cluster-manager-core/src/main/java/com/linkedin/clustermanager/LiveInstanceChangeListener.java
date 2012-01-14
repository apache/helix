package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.model.LiveInstance;

public interface LiveInstanceChangeListener
{

  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
      NotificationContext changeContext);

}

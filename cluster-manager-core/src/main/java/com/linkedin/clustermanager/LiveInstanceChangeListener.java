package com.linkedin.clustermanager;

import java.util.List;

public interface LiveInstanceChangeListener
{

  public void onLiveInstanceChange(List<ZNRecord> liveInstances,
      NotificationContext changeContext);

}

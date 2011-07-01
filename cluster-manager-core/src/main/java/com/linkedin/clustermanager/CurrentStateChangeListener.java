package com.linkedin.clustermanager;

import java.util.List;

public interface CurrentStateChangeListener
{

  public void onStateChange(String instanceName, List<ZNRecord> statesInfo,
      NotificationContext changeContext);

}

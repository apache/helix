package com.linkedin.clustermanager;

import java.util.List;

public interface HealthStateChangeListener
{

  public void onHealthChange(String instanceName,  List<ZNRecord> reports,
	  NotificationContext changeContext);
  
}

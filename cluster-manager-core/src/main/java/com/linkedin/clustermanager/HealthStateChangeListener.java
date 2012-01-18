package com.linkedin.clustermanager;

import java.util.List;

import com.linkedin.clustermanager.model.HealthStat;
import com.linkedin.clustermanager.model.Message;

public interface HealthStateChangeListener
{

  public void onHealthChange(String instanceName,  List<HealthStat> reports,
	  NotificationContext changeContext);
  /*  
  public void onHealthChange(String instanceName,  List<ZNRecord> reports,
		  NotificationContext changeContext);
  */
}

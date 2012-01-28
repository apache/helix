package com.linkedin.helix;

import java.util.List;

import com.linkedin.helix.model.HealthStat;
import com.linkedin.helix.model.Message;

public interface HealthStateChangeListener
{

  public void onHealthChange(String instanceName,  List<HealthStat> reports,
	  NotificationContext changeContext);
  /*  
  public void onHealthChange(String instanceName,  List<ZNRecord> reports,
		  NotificationContext changeContext);
  */
}

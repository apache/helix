package com.linkedin.helix.monitoring.mbeans;

import com.linkedin.helix.monitoring.SensorNameGetter;

public interface MessageQueueMonitorMBean extends SensorNameGetter
{
  public double getMaxMessageQueueSize();
  
  public double getMeanMessageQueueSize();

}

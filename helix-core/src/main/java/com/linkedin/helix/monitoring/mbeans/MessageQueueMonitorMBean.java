package com.linkedin.helix.monitoring.mbeans;

import com.linkedin.helix.monitoring.SensorNameProvider;

public interface MessageQueueMonitorMBean extends SensorNameProvider
{
  /**
   * Get the max message queue size
   * @return
   */
  public double getMaxMessageQueueSize();
  
  /**
   * Get the mean message queue size
   * @return
   */
  public double getMeanMessageQueueSize();

}

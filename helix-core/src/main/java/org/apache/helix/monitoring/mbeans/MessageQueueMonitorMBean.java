package org.apache.helix.monitoring.mbeans;

import org.apache.helix.monitoring.SensorNameProvider;

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

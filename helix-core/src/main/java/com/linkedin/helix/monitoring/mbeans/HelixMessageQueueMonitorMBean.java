package com.linkedin.helix.monitoring.mbeans;

public interface HelixMessageQueueMonitorMBean
{
  public double getMaxMessageQueueSize();
  
  public double getMeanMessageQueueSize();

}

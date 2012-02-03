package com.linkedin.helix.monitoring.mbeans;

public interface ClusterStatusMonitorMBean
{
  public long getLiveInstanceGauge();
  
  public long getInstancesGauge();
}

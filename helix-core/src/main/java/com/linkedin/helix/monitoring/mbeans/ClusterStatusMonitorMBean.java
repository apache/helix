package com.linkedin.helix.monitoring.mbeans;

public interface ClusterStatusMonitorMBean
{
  public long getDownInstanceGauge();
  
  public long getInstancesGauge();
}

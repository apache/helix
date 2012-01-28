package com.linkedin.helix.monitoring.mbeans;

import com.linkedin.helix.monitoring.annotations.Description;

public interface ClusterStatusMonitorMBean
{
  @Description("Number of live instances")
  public long getLiveInstanceGauge();
  
  @Description("Number of total instances")
  public long getInstancesGauge();
}

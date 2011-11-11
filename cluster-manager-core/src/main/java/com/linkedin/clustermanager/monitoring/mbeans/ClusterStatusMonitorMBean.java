package com.linkedin.clustermanager.monitoring.mbeans;

import com.linkedin.clustermanager.monitoring.annotations.Description;

public interface ClusterStatusMonitorMBean
{
  @Description("Number of live instances")
  public long getLiveInstanceGauge();
  
  @Description("Number of total instances")
  public long getInstancesGauge();
}

package com.linkedin.clustermanager.monitoring.mbeans;

public interface ClusterStatusMonitorMBean
{
  @Description("Number of live instances")
  public long getLiveInstanceCount();
  
  @Description("Number of total instances")
  public long getInstancesCount();
}

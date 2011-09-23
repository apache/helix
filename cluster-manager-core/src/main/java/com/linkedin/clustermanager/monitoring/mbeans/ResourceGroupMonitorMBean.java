package com.linkedin.clustermanager.monitoring.mbeans;

public interface ResourceGroupMonitorMBean
{
  @Description("Number of total resource keys")
  public long getResourceKeyGauge();
  
  @Description("Number of resource keys in error state")
  public long getErrorResouceKeyGauge();
  
  @Description("Difference between ideal state and external view")
  public long getDifferenceWithIdealStateGauge();
  
  @Description("Number of resource keys in external view")
  public long getExternalViewResourceKeyGauge();
}

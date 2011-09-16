package com.linkedin.clustermanager.monitoring.mbeans;

public interface ResourceGroupMonitorMBean
{
  @Description("Number of total resource keys")
  public long getResourceKeyCount();
  
  @Description("Number of resource keys in error state")
  public long getErrorResouceKeyCount();
  
  @Description("Difference between ideal state and external view")
  public long getDifferenceWithIdealStateCount();
  
  @Description("Number of resource keys in external view")
  public long getExternalViewResourceKeyCount();
}

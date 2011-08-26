package com.linkedin.clustermanager.monitoring.mbeans;

public interface ResourceGroupMonitorMBean
{
  @Description("Number of total resource keys")
  public long getNumberOfResourceKeys();
  
  @Description("Number of resource keys in error state")
  public long getNumberOfErrorResouceKeys();
  
  @Description("Difference between ideal state and external view")
  public long getDifferenceNumberWithIdealState();
  
  @Description("Number of resource keys in external view")
  public long getNumberOfResourceKeysInExternalView();
}

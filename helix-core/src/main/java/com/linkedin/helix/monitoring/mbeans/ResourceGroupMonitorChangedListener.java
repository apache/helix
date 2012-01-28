package com.linkedin.helix.monitoring.mbeans;

/*
 * Note: Since drop resource group is not supported in release, onResourceGroupMonitorRemoved is not 
 * supported.
 * */
public interface ResourceGroupMonitorChangedListener
{
  public void onResourceGroupMonitorAdded(ResourceGroupMonitor newResourceGroupMonitor);
  
}

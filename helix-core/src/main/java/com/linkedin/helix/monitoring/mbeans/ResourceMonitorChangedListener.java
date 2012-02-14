package com.linkedin.helix.monitoring.mbeans;

/*
 * Note: Since drop resource is not supported in release, onResourceMonitorRemoved is not 
 * supported.
 * */
public interface ResourceMonitorChangedListener
{
  public void onResourceMonitorAdded(ResourceMonitor newResourceMonitor);
  
}

package com.linkedin.helix.monitoring.mbeans;


public interface ResourceGroupMonitorMBean
{
  public long getResourceKeyGauge();
  
  public long getErrorResouceKeyGauge();
  
  public long getDifferenceWithIdealStateGauge();
  
  public long getExternalViewResourceKeyGauge();
}

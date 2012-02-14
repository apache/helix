package com.linkedin.helix.monitoring.mbeans;


public interface ResourceMonitorMBean
{
  public long getPartitionGauge();
  
  public long getErrorPartitionGauge();
  
  public long getDifferenceWithIdealStateGauge();
  
  public long getExternalViewPartitionGauge();
}

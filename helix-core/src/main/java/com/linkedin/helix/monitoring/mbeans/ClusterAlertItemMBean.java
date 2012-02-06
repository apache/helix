package com.linkedin.helix.monitoring.mbeans;

public interface ClusterAlertItemMBean
{
  String getSensorName();
  
  double getAlertValue();
  
  int getAlertFired();
}

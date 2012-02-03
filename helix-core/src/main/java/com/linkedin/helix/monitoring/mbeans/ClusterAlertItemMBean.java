package com.linkedin.helix.monitoring.mbeans;

public interface ClusterAlertItemMBean
{
  String getName();
  
  double getAlertValue();
  
  int getAlertFired();
}

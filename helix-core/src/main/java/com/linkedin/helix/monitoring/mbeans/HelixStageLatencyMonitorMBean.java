package com.linkedin.helix.monitoring.mbeans;

public interface HelixStageLatencyMonitorMBean
{
  public long getMaxHealthStatsAggStgLatency();

  public long getMeanHealthStatsAggStgLatency();

  public long get95HealthStatsAggStgLatency();
}

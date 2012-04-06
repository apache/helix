package com.linkedin.helix.monitoring.mbeans;

public interface HelixStageLatencyMonitorMBean
{
  public long getMaxStgLatency();

  public long getMeanStgLatency();

  public long get95StgLatency();
}

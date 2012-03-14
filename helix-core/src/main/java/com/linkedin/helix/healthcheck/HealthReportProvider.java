package com.linkedin.helix.healthcheck;

import java.util.Map;

public abstract class HealthReportProvider
{
  public static final String _defaultPerfCounters = "defaultPerfCounters";

  public abstract Map<String, String> getRecentHealthReport();

  public Map<String, Map<String, String>> getRecentPartitionHealthReport()
  {
    return null;
  }

  public abstract void resetStats();

  public String getReportName()
  {
    return _defaultPerfCounters;
  }

}

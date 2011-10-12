package com.linkedin.clustermanager.healthcheck;

import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;

public abstract class HealthReportProvider
{
  public static final String _defaultPerfCounters = "defaultPerfCounters";
  
  public abstract Map<String, String> getRecentHealthReport();
  
  
  public Map<String, Map<String, String>> getRecentPartitionHealthReport()
  {
	return null;
  }
  
  
  public String getReportName()
  {
    return _defaultPerfCounters;
  }
}

package com.linkedin.clustermanager.healthcheck;

import java.util.Map;

public abstract class HealthReportProvider
{
  public static final String _defaultPerfCounters = "defaultPerfCounters";
  
  public abstract Map<String, String> getRecentHealthReport();
  
  
  public Map<String, Map<String, String>> getRecentPartitionHealthReport()
  {
	return null;
  }
  
  public void resetPartitionStats() 
  {
	  //default is do nothing
  }
  
  public String getReportName()
  {
    return _defaultPerfCounters;
  }

}

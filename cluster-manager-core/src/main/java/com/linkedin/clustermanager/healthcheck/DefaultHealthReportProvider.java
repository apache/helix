package com.linkedin.clustermanager.healthcheck;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ZNRecord;

class DefaultHealthReportProvider extends HealthReportProvider
{
  private static final Logger _logger = Logger.getLogger(DefaultHealthReportProvider.class);
  
  public final static String _availableCPUs = "availableCPUs";
  public final static String _freePhysicalMemory = "freePhysicalMemory";
  public final static String _totalJvmMemory = "totalJvmMemory";
  public final static String _freeJvmMemory = "freeJvmMemory";
  public final static String _averageSystemLoad = "averageSystemLoad";
  
  public DefaultHealthReportProvider()
  {}

  @Override
  public Map<String, String> getRecentHealthReport()
  {
    OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
    long freeJvmMemory = Runtime.getRuntime().freeMemory();
    long totalJvmMemory = Runtime.getRuntime().totalMemory();
    int availableCPUs = osMxBean.getAvailableProcessors();
    double avgSystemLoad = osMxBean.getSystemLoadAverage();
    long freePhysicalMemory = Long.MAX_VALUE;
    
    try
    {
//      if( osMxBean instanceof  com.sun.management.OperatingSystemMXBean)
//      {
//        com.sun.management.OperatingSystemMXBean sunOsMxBean 
//          = (com.sun.management.OperatingSystemMXBean) osMxBean;
//        freePhysicalMemory = sunOsMxBean.getFreePhysicalMemorySize();
//      }
    }
    catch (Throwable t) 
    {
      _logger.error(t);
    }
    
    Map<String, String> result = new TreeMap<String, String>();
    
    result.put(_availableCPUs, "" + availableCPUs);
    result.put(_freePhysicalMemory, "" + freePhysicalMemory);
    result.put(_freeJvmMemory, "" + freeJvmMemory);
    result.put(_totalJvmMemory, "" + totalJvmMemory);
    result.put(_averageSystemLoad, "" + avgSystemLoad);
    
    return result;
  }

@Override
public void resetStats() {
	// TODO Auto-generated method stub
	
}
}
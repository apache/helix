package com.linkedin.clustermanager.healthcheck;

import java.util.Date;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ZNRecord;
@Deprecated
public class DefaultPerfCounters extends ZNRecord
{
  private static final Logger _logger = Logger.getLogger(DefaultPerfCounters.class);
  
  public final static String _availableCPUs = "availableCPUs";
  public final static String _freePhysicalMemory = "freePhysicalMemory";
  public final static String _totalJvmMemory = "totalJvmMemory";
  public final static String _freeJvmMemory = "freeJvmMemory";
  public final static String _averageSystemLoad = "averageSystemLoad";
  
  public DefaultPerfCounters(
      String instanceName,
      long availableCPUs,
      long freePhysicalMemory,
      long freeJvmMemory,
      long totalJvmMemory,
      double averageSystemLoad
    )
  {
    setId("DefaultPerfCounters");
    setSimpleField("instanceName", instanceName);
    setSimpleField("createTime", new Date().toString());
    
    setSimpleField(_availableCPUs, "" + availableCPUs);
    setSimpleField(_freePhysicalMemory, "" + freePhysicalMemory);
    setSimpleField(_freeJvmMemory, "" + freeJvmMemory);
    setSimpleField(_totalJvmMemory, "" + totalJvmMemory);
    setSimpleField(_averageSystemLoad, "" + averageSystemLoad);
  }
  
  public long getAvailableCpus()
  {
    return getSimpleLongVal(_availableCPUs);
  }
  
  public double getAverageSystemLoad()
  {
    return getSimpleDoubleVal(_averageSystemLoad);
  }
  
  public long getTotalJvmMemory()
  {
    return getSimpleLongVal(_totalJvmMemory);
  }
  
  public long getFreeJvmMemory()
  {
    return getSimpleLongVal(_freeJvmMemory);
  }
  
  public long getFreePhysicalMemory()
  {
    return getSimpleLongVal(_freePhysicalMemory);
  }
  
  long getSimpleLongVal(String key)
  {
    String strVal = getSimpleField(key);
    if(strVal == null)
    {
      return 0;
    }
    try
    {
      return Long.parseLong(strVal);
    }
    catch(Exception e)
    {
      _logger.warn(e);
      return 0;
    }
  }
  
  double getSimpleDoubleVal(String key)
  {
    String strVal = getSimpleField(key);
    if(strVal == null)
    {
      return 0;
    }
    try
    {
      return Double.parseDouble(strVal);
    }
    catch(Exception e)
    {
      _logger.warn(e);
      return 0;
    }
  }
}

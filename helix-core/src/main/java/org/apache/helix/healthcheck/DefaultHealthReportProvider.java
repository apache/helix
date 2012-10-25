package org.apache.helix.healthcheck;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;


class DefaultHealthReportProvider extends HealthReportProvider
{
  private static final Logger _logger = Logger
      .getLogger(DefaultHealthReportProvider.class);

  public final static String _availableCPUs = "availableCPUs";
  public final static String _freePhysicalMemory = "freePhysicalMemory";
  public final static String _totalJvmMemory = "totalJvmMemory";
  public final static String _freeJvmMemory = "freeJvmMemory";
  public final static String _averageSystemLoad = "averageSystemLoad";

  public DefaultHealthReportProvider()
  {
  }

  @Override
  public Map<String, String> getRecentHealthReport()
  {
    OperatingSystemMXBean osMxBean = ManagementFactory
        .getOperatingSystemMXBean();
    long freeJvmMemory = Runtime.getRuntime().freeMemory();
    long totalJvmMemory = Runtime.getRuntime().totalMemory();
    int availableCPUs = osMxBean.getAvailableProcessors();
    double avgSystemLoad = osMxBean.getSystemLoadAverage();
    long freePhysicalMemory = Long.MAX_VALUE;

    try
    {
      // if( osMxBean instanceof com.sun.management.OperatingSystemMXBean)
      // {
      // com.sun.management.OperatingSystemMXBean sunOsMxBean
      // = (com.sun.management.OperatingSystemMXBean) osMxBean;
      // freePhysicalMemory = sunOsMxBean.getFreePhysicalMemorySize();
      // }
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
  public Map<String, Map<String, String>> getRecentPartitionHealthReport()
  {
    Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
    
    result.put(getReportName(), getRecentHealthReport());
    return result;
  }

  @Override
  public void resetStats()
  {
    // TODO Auto-generated method stub

  }
}
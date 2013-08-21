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

import java.util.Date;

import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;

@Deprecated
public class DefaultPerfCounters extends ZNRecord {
  private static final Logger _logger = Logger.getLogger(DefaultPerfCounters.class);

  public final static String _availableCPUs = "availableCPUs";
  public final static String _freePhysicalMemory = "freePhysicalMemory";
  public final static String _totalJvmMemory = "totalJvmMemory";
  public final static String _freeJvmMemory = "freeJvmMemory";
  public final static String _averageSystemLoad = "averageSystemLoad";

  public DefaultPerfCounters(String instanceName, long availableCPUs, long freePhysicalMemory,
      long freeJvmMemory, long totalJvmMemory, double averageSystemLoad) {
    super("DefaultPerfCounters");
    setSimpleField("instanceName", instanceName);
    setSimpleField("createTime", new Date().toString());

    setSimpleField(_availableCPUs, "" + availableCPUs);
    setSimpleField(_freePhysicalMemory, "" + freePhysicalMemory);
    setSimpleField(_freeJvmMemory, "" + freeJvmMemory);
    setSimpleField(_totalJvmMemory, "" + totalJvmMemory);
    setSimpleField(_averageSystemLoad, "" + averageSystemLoad);
  }

  public long getAvailableCpus() {
    return getSimpleLongVal(_availableCPUs);
  }

  public double getAverageSystemLoad() {
    return getSimpleDoubleVal(_averageSystemLoad);
  }

  public long getTotalJvmMemory() {
    return getSimpleLongVal(_totalJvmMemory);
  }

  public long getFreeJvmMemory() {
    return getSimpleLongVal(_freeJvmMemory);
  }

  public long getFreePhysicalMemory() {
    return getSimpleLongVal(_freePhysicalMemory);
  }

  long getSimpleLongVal(String key) {
    String strVal = getSimpleField(key);
    if (strVal == null) {
      return 0;
    }
    try {
      return Long.parseLong(strVal);
    } catch (Exception e) {
      _logger.warn(e);
      return 0;
    }
  }

  double getSimpleDoubleVal(String key) {
    String strVal = getSimpleField(key);
    if (strVal == null) {
      return 0;
    }
    try {
      return Double.parseDouble(strVal);
    } catch (Exception e) {
      _logger.warn(e);
      return 0;
    }
  }
}

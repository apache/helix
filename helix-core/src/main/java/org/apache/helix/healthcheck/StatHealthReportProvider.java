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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class StatHealthReportProvider extends HealthReportProvider {

  private static final Logger _logger = Logger.getLogger(StatHealthReportProvider.class);

  /*
   * public final static String _testStat = "testStat"; public final static
   * String _readLatencyStat = "readLatencyStat"; public final static String
   * _requestCountStat = "requestCountStat"; public final static String
   * _partitionRequestCountStat = "partitionRequestCountStat";
   */

  public static final String REPORT_NAME = "ParticipantStats";
  public String _reportName = REPORT_NAME;

  public static final String STAT_VALUE = "value";
  public static final String TIMESTAMP = "timestamp";

  public int readLatencyCount = 0;
  public double readLatencySum = 0;

  public int requestCount = 0;

  // private final Map<String, String> _partitionCountsMap = new HashMap<String,
  // String>();

  // private final Map<String, HashMap<String,String>> _partitionStatMaps = new
  // HashMap<String, HashMap<String,String>>();
  private final ConcurrentHashMap<String, String> _statsToValues =
      new ConcurrentHashMap<String, String>();
  private final ConcurrentHashMap<String, String> _statsToTimestamps =
      new ConcurrentHashMap<String, String>();

  public StatHealthReportProvider() {
  }

  @Override
  public Map<String, String> getRecentHealthReport() {
    return null;
  }

  // TODO: function is misnamed, but return type is what I want
  @Override
  public Map<String, Map<String, String>> getRecentPartitionHealthReport() {
    Map<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
    for (String stat : _statsToValues.keySet()) {
      Map<String, String> currStat = new HashMap<String, String>();
      /*
       * currStat.put(Stat.OP_TYPE, stat._opType);
       * currStat.put(Stat.MEASUREMENT_TYPE, stat._measurementType);
       * currStat.put(Stat.NODE_NAME, stat._nodeName);
       * currStat.put(Stat.PARTITION_NAME, stat._partitionName);
       * currStat.put(Stat.RESOURCE_NAME, stat._resourceName);
       * currStat.put(Stat.RETURN_STATUS, stat._returnStatus);
       * currStat.put(Stat.METRIC_NAME, stat._metricName);
       * currStat.put(Stat.AGG_TYPE, stat._aggTypeName);
       */
      currStat.put(TIMESTAMP, _statsToTimestamps.get(stat));
      currStat.put(STAT_VALUE, _statsToValues.get(stat));
      result.put(stat, currStat);
    }
    return result;
  }

  public boolean contains(Stat inStat) {
    return _statsToValues.containsKey(inStat);
  }

  public Set<String> keySet() {
    return _statsToValues.keySet();
  }

  public String getStatValue(Stat inStat) {
    return _statsToValues.get(inStat);
  }

  public long getStatTimestamp(Stat inStat) {
    return Long.parseLong(_statsToTimestamps.get(inStat));
  }

  /*
   * public String getStatValue(String opType, String measurementType, String
   * resourceName, String partitionName, String nodeName, boolean
   * createIfMissing) { Stat rs = new Stat(opType, measurementType,
   * resourceName, partitionName, nodeName); String val =
   * _statsToValues.get(rs); if (val == null && createIfMissing) { val = "0";
   * _statsToValues.put(rs, val); } return val; }
   */

  public void writeStat(String statName, String val, String timestamp) {
    _statsToValues.put(statName, val);
    _statsToTimestamps.put(statName, timestamp);
  }

  /*
   * public void setStat(Stat es, String val, String timestamp) { writeStat(es,
   * val, timestamp); }
   * public void setStat(String opType, String measurementType, String
   * resourceName, String partitionName, String nodeName, double val, String
   * timestamp) { Stat rs = new Stat(opType, measurementType, resourceName,
   * partitionName, nodeName); writeStat(rs, String.valueOf(val), timestamp); }
   */

  public void incrementStat(String statName, String timestamp) {
    // Stat rs = new Stat(opType, measurementType, resourceName, partitionName,
    // nodeName);
    String val = _statsToValues.get(statName);
    if (val == null) {
      val = "0";
    } else {
      val = String.valueOf(Double.parseDouble(val) + 1);
    }
    writeStat(statName, val, timestamp);
  }

  public int size() {
    return _statsToValues.size();
  }

  public void resetStats() {
    _statsToValues.clear();
    _statsToTimestamps.clear();
  }

  public void setReportName(String name) {
    _reportName = name;
  }

  public String getReportName() {
    return _reportName;
  }
}

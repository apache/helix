package org.apache.helix.model;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.Message.Attributes;

/**
 * Represents a set of properties that can be queried to determine the health of instances on a
 * Helix-managed cluster
 */
public class HealthStat extends HelixProperty {
  public static final String VALUE_NAME = "value";
  public static final String TIMESTAMP_NAME = "TimeStamp";
  public static final String statFieldDelim = ".";

  /**
   * Queryable health statistic properties
   */
  public enum HealthStatProperty {
    FIELDS
  }

  /**
   * Instantiate with an identifier
   * @param id the name of these statistics
   */
  public HealthStat(String id) {
    super(id);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record a ZNRecord corresponding to health statistics
   */
  public HealthStat(ZNRecord record) {
    super(record);
    if (getCreateTimeStamp() == 0) {
      _record.setLongField(Attributes.CREATE_TIMESTAMP.toString(), new Date().getTime());
    }
  }

  /**
   * Get when these statistics were last modified
   * @return a UNIX timestamp
   */
  public long getLastModifiedTimeStamp() {
    return _record.getModifiedTime();
  }

  /**
   * Get when these statistics were created
   * @return a UNIX timestamp
   */
  public long getCreateTimeStamp() {
    return _record.getLongField(Attributes.CREATE_TIMESTAMP.toString(), 0L);
  }

  /**
   * Get the value of a test field corresponding to a request count
   * @return the number of requests
   */
  public String getTestField() {
    return _record.getSimpleField("requestCountStat");
  }

  /**
   * Set a group of heath statistics, grouped by the statistic
   * @param healthFields a map of statistic name, the corresponding entity, and the value
   */
  public void setHealthFields(Map<String, Map<String, String>> healthFields) {
    _record.setMapFields(healthFields);
  }

  /**
   * Create a key based on a parent key, instance, and statistic
   * @param instance the instance for which these statistics exist
   * @param parentKey the originating key
   * @param statName the statistic
   * @return a unified key
   */
  public String buildCompositeKey(String instance, String parentKey, String statName) {
    String delim = statFieldDelim;
    return instance + delim + parentKey + delim + statName;
  }

  /**
   * Get all the health statistics for a given instance
   * @param instanceName the instance for which to get health statistics
   * @return a map of (instance and statistic, value or timestamp, value) triples
   */
  public Map<String, Map<String, String>> getHealthFields(String instanceName) // ,
                                                                               // String
                                                                               // timestamp)
  {
    // XXX: need to do some conversion of input format to the format that stats
    // computation wants
    Map<String, Map<String, String>> currMapFields = _record.getMapFields();
    Map<String, Map<String, String>> convertedMapFields =
        new HashMap<String, Map<String, String>>();
    for (String key : currMapFields.keySet()) {
      Map<String, String> currMap = currMapFields.get(key);
      String timestamp = _record.getStringField(TIMESTAMP_NAME, "-1");
      for (String subKey : currMap.keySet()) {
        if (subKey.equals("StatsHolder.TIMESTAMP_NAME")) { // don't want to get timestamp again
          continue;
        }
        String compositeKey = buildCompositeKey(instanceName, key, subKey);
        String value = currMap.get(subKey);
        Map<String, String> convertedMap = new HashMap<String, String>();
        convertedMap.put(VALUE_NAME, value);
        convertedMap.put(TIMESTAMP_NAME, timestamp);
        convertedMapFields.put(compositeKey, convertedMap);
      }
    }
    return convertedMapFields;
  }

  @Override
  public boolean isValid() {
    // TODO Auto-generated method stub
    return true;
  }
}
/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.alerts.ExpressionParser;
import com.linkedin.helix.alerts.StatsHolder;
import com.linkedin.helix.model.Message.Attributes;

public class HealthStat extends HelixProperty 
{
  public enum HealthStatProperty
  {
    FIELDS
  }
  private static final Logger _logger = Logger.getLogger(HealthStat.class.getName());

  public HealthStat(String id)
  {
    super(id);
  }

  public HealthStat(ZNRecord record)
    {
      super(record);
      if(getCreateTimeStamp() == 0)
      {
        _record.setSimpleField(Attributes.CREATE_TIMESTAMP.toString(), ""
            + new Date().getTime());
      }
    }

  public long getLastModifiedTimeStamp()
  {
    return _record.getModifiedTime();
  }

  public long getCreateTimeStamp()
  {
    if (_record.getSimpleField(Attributes.CREATE_TIMESTAMP.toString()) == null)
    {
      return 0;
    }
    try
    {
      return Long.parseLong(_record.getSimpleField(Attributes.CREATE_TIMESTAMP.toString()));
    } catch (Exception e)
    {
      return 0;
    }
  }
  
  public String getTestField()
  {
    return _record.getSimpleField("requestCountStat");
  }
  
  public void setHealthFields(Map<String, Map<String, String>> healthFields)
  {
    _record.setMapFields(healthFields);
  }
  
  public String buildCompositeKey(String instance, String parentKey, String statName ) {
    String delim = ExpressionParser.statFieldDelim;
    return instance+delim+parentKey+delim+statName;
  }

  public Map<String, Map<String, String>> getHealthFields(String instanceName) // ,
                                                                               // String
                                                                               // timestamp)
  {
    // XXX: need to do some conversion of input format to the format that stats
    // computation wants
    Map<String, Map<String, String>> currMapFields = _record.getMapFields();
    Map<String, Map<String, String>> convertedMapFields = new HashMap<String, Map<String, String>>();
    for (String key : currMapFields.keySet())
    {
      Map<String, String> currMap = currMapFields.get(key);
      String timestamp = "-1";
      if (_record.getSimpleFields().keySet().contains(StatsHolder.TIMESTAMP_NAME))
      {
        timestamp = _record.getSimpleField(StatsHolder.TIMESTAMP_NAME);
      }
      for (String subKey : currMap.keySet())
      {
        if (subKey.equals("StatsHolder.TIMESTAMP_NAME"))
        { // don't want to get timestamp again
          continue;
        }
        String compositeKey = buildCompositeKey(instanceName, key, subKey);
        String value = currMap.get(subKey);
        Map<String, String> convertedMap = new HashMap<String, String>();
        convertedMap.put(StatsHolder.VALUE_NAME, value);
        convertedMap.put(StatsHolder.TIMESTAMP_NAME, timestamp);
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

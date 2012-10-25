package org.apache.helix.josql;

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

import java.util.Map;

import org.apache.helix.ZNRecord;
import org.josql.functions.AbstractFunctionHandler;



public class ZNRecordJosqlFunctionHandler extends AbstractFunctionHandler
{
  public boolean hasSimpleField(ZNRecord record, String fieldName, String field)
  {
    if(!record.getSimpleFields().containsKey(fieldName))
    {
      return false;
    }
    return field.equals(record.getSimpleField(fieldName));
  }
  
  public boolean hasListField(ZNRecord record, String fieldName, String field)
  {
    if(!record.getListFields().containsKey(fieldName))
    {
      return false;
    }
    return record.getListField(fieldName).contains(field);
  }
  
  public boolean hasMapFieldValue(ZNRecord record, String fieldName, String mapKey, String mapValue)
  {
    if(!record.getMapFields().containsKey(fieldName))
    {
      return false;
    }
    if(record.getMapField(fieldName).containsKey(mapKey))
    {
      return record.getMapField(fieldName).get(mapKey).equals(mapValue);
    }
    return false;
  }
  
  public boolean hasMapFieldKey(ZNRecord record, String fieldName, String mapKey)
  {
    if(!record.getMapFields().containsKey(fieldName))
    {
      return false;
    }
    return record.getMapField(fieldName).containsKey(mapKey);
  }
  
  public String getMapFieldValue(ZNRecord record, String fieldName, String mapKey)
  {
    if(record.getMapFields().containsKey(fieldName))
    {
      return record.getMapField(fieldName).get(mapKey);
    }
    return null;
  }
  
  public String getSimpleFieldValue(ZNRecord record, String key)
  {
    return record.getSimpleField(key);
  }
  
  public ZNRecord getZNRecordFromMap(Map<String, ZNRecord> recordMap, String key)
  {
    return recordMap.get(key);
  }
  
  public ZNRecord getZNRecordFromMap(Map<String, Map<String, ZNRecord>> recordMap, String key, String subKey)
  {
    return recordMap.get(key).get(subKey);
  }
}

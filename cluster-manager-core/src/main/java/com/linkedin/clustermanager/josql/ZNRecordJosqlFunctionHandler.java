package com.linkedin.clustermanager.josql;

import java.util.Map;

import org.josql.functions.AbstractFunctionHandler;

import com.linkedin.clustermanager.ZNRecord;

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
}

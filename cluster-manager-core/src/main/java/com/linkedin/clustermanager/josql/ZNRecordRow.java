package com.linkedin.clustermanager.josql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;

/**
 * A Normalized form of ZNRecord 
 * */
public class ZNRecordRow
{
  // "Field names" in the flattened ZNRecord
  public static final String SIMPLE_KEY = "simpleKey";
  public static final String SIMPLE_VALUE = "simpleValue";
  
  public static final String LIST_KEY = "listKey";
  public static final String LIST_VALUE = "listValue";
  
  public static final String MAP_KEY = "mapKey";
  public static final String MAP_SUBKEY = "mapSubKey";
  public static final String MAP_VALUE = "mapValue";
  public static final String ZNRECORD_ID = "recordId";
  
  
  final Map<String, String> _rowDataMap = new HashMap<String, String>();
  
  public ZNRecordRow()
  {
    _rowDataMap.put(SIMPLE_KEY, "");
    _rowDataMap.put(SIMPLE_VALUE, "");
    _rowDataMap.put(LIST_KEY, "");
    _rowDataMap.put(LIST_VALUE, "");
    _rowDataMap.put(MAP_KEY, "");
    _rowDataMap.put(MAP_SUBKEY, "");
    _rowDataMap.put(MAP_VALUE, "");
    _rowDataMap.put(ZNRECORD_ID, "");
  }
  
  public String getField(String rowField)
  {
    return _rowDataMap.get(rowField);
  }
  
  public void putField(String fieldName, String fieldValue)
  {
    _rowDataMap.put(fieldName, fieldValue);
  }
  
  public String getSimpleKey()
  {
    return getField(SIMPLE_KEY);
  }
  
  public String getSimpleValue()
  {
    return getField(SIMPLE_VALUE);
  }
  
  public String getListKey()
  {
    return getField(LIST_KEY);
  }
  
  public String getListValue()
  {
    return getField(LIST_VALUE);
  }
  
  public String getMapKey()
  {
    return getField(MAP_KEY);
  }
  
  public String getMapSubKey()
  {
    return getField(MAP_SUBKEY);
  }
  
  public String getMapValue()
  {
    return getField(MAP_VALUE);
  }
  
  public String getRecordId()
  {
    return getField(ZNRECORD_ID);
  }
  
  /* Josql function handlers */
  public static String getField(ZNRecordRow row, String rowField)
  {
    return row.getField(rowField);
  }
  
  public static List<ZNRecordRow> convertSimpleFields(ZNRecord record)
  {
    List<ZNRecordRow> result = new ArrayList<ZNRecordRow>();
    for(String key : record.getSimpleFields().keySet())
    {
      ZNRecordRow row = new ZNRecordRow();
      row.putField(ZNRECORD_ID, record.getId());
      row.putField(SIMPLE_KEY, key);
      row.putField(SIMPLE_VALUE, record.getSimpleField(key));
      result.add(row);
    }
    return result;
  }
  
  public static List<ZNRecordRow> convertListFields(ZNRecord record)
  {
    List<ZNRecordRow> result = new ArrayList<ZNRecordRow>();
    for(String key : record.getListFields().keySet())
    {
      for(String value : record.getListField(key))
      {
        ZNRecordRow row = new ZNRecordRow();
        row.putField(ZNRECORD_ID, record.getId());
        row.putField(LIST_KEY, key);
        row.putField(LIST_VALUE, record.getSimpleField(key));
        result.add(row);
      }
    }
    return result;
  }
  
  public static List<ZNRecordRow> convertMapFields(ZNRecord record)
  {
    List<ZNRecordRow> result = new ArrayList<ZNRecordRow>();
    for(String key0 : record.getMapFields().keySet())
    {
      for(String key1 : record.getMapField(key0).keySet())
      {
        ZNRecordRow row = new ZNRecordRow();
        row.putField(ZNRECORD_ID, record.getId());
        row.putField(MAP_KEY, key0);
        row.putField(MAP_SUBKEY, key1);
        row.putField(MAP_VALUE, record.getMapField(key0).get(key1));
        result.add(row);
      }
    }
    return result;
  }
  
  public static List<ZNRecordRow> flat(ZNRecord record)
  {
    List<ZNRecordRow> result = convertMapFields(record);
    result.addAll(convertListFields(record));
    result.addAll(convertSimpleFields(record));
    return result;
  }
  
  public static List<ZNRecordRow> flat(Collection<ZNRecord> recordList)
  {
    List<ZNRecordRow> result = new ArrayList<ZNRecordRow>();
    for(ZNRecord record : recordList)
    {
      result.addAll(flat(record));
    }
    return result;
  }
}

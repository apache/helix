package com.linkedin.clustermanager.util;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ZNRecord;

//todo find a proper place for these methods
public final class ZNRecordUtil
{
  private static final Logger logger = Logger.getLogger(ZNRecordUtil.class
      .getName());

  private ZNRecordUtil()
  {
  }

  public static ZNRecord find(String id, List<ZNRecord> list)
  {
    for (ZNRecord record : list)
    {
      if (record.getId() != null && record.getId().equals(id))
      {
        return record;
      }
    }
    return null;
  }

  public static Map<String, ZNRecord> convertListToMap(List<ZNRecord> recordList)
  {
    Map<String, ZNRecord> recordMap = new HashMap<String, ZNRecord>();
    for (ZNRecord record : recordList)
    {
      if (record.getId() != null)
      {
        recordMap.put(record.getId(), record);
      }
    }
    return recordMap;
  }

  public static <T extends Object> Map<String, T> convertListToTypedMap(
      List<ZNRecord> recordList, Class<T> clazz)
  {

    Map<String, T> map = new HashMap<String, T>();
    for (ZNRecord record : recordList)
    {
      if (record.getId() == null)
      {
        logger.error("Invalid record: Id missing in " + record);
        continue;
      }
      try
      {

        Constructor<T> constructor = clazz.getConstructor(new Class[]
        { ZNRecord.class });
        T instance = constructor.newInstance(record);
        map.put(record.getId(), instance);
      } catch (Exception e)
      {
        logger.error(
            "Error creating an Object of type:" + clazz.getCanonicalName(), e);
      }
    }
    return map;
  }
}

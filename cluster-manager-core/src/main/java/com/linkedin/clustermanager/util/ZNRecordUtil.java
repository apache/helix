package com.linkedin.clustermanager.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;

//todo find a proper place for these methods
public final class ZNRecordUtil
{
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
}

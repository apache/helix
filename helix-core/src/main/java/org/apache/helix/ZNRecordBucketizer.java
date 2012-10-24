package org.apache.helix;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ZNRecordBucketizer
{
  private static Logger LOG = Logger.getLogger(ZNRecordBucketizer.class);
  final int             _bucketSize;

  public ZNRecordBucketizer(int bucketSize)
  {
    if (bucketSize <= 0)
    {
      LOG.debug("bucketSize <= 0 (was " + bucketSize
          + "). Set to 0 to use non-bucketized HelixProperty.");
      bucketSize = 0;
    }

    _bucketSize = bucketSize;
  }

  /**
   * Calculate bucketName in form of "resourceName_p{startPartition}-p{endPartition}
   * 
   * @param partitionName
   * @return
   */
  public String getBucketName(String key)
  {
    if (_bucketSize == 0)
    {
      // no bucketize
      return null;
    }

    int idx = key.lastIndexOf('_');
    if (idx < 0)
    {
      throw new IllegalArgumentException("Could NOT find partition# in " + key
          + ". partitionName should be in format of resourceName_partition#");
    }

    try
    {
      int partitionNb = Integer.parseInt(key.substring(idx + 1));
      int bucketNb = partitionNb / _bucketSize;
      int startPartition = bucketNb * _bucketSize;
      int endPartition = bucketNb * _bucketSize + (_bucketSize - 1);
      return key.substring(0, idx) + "_p" + startPartition + "-p" + endPartition;
    }
    catch (NumberFormatException e)
    {
      throw new IllegalArgumentException("Could NOT parse partition# ("
          + key.substring(idx + 1) + ") in " + key);
    }
  }

  public Map<String, ZNRecord> bucketize(ZNRecord record)
  {
    Map<String, ZNRecord> map = new HashMap<String, ZNRecord>();
    if (_bucketSize == 0)
    {
      map.put(record.getId(), record);
      return map;
    }
    
    // bucketize list field
    for (String partitionName : record.getListFields().keySet())
    {
      String bucketName = getBucketName(partitionName);
      if (bucketName != null)
      {
        if (!map.containsKey(bucketName))
        {
          map.put(bucketName, new ZNRecord(bucketName));
        }
        ZNRecord bucketizedRecord = map.get(bucketName);
        bucketizedRecord.setListField(partitionName, record.getListField(partitionName));
      }
      else
      {
        LOG.error("Can't bucketize " + partitionName + " in list field");
      }
    }

    // bucketize map field
    for (String partitionName : record.getMapFields().keySet())
    {
      String bucketName = getBucketName(partitionName);
      if (bucketName != null)
      {
        if (!map.containsKey(bucketName))
        {
          map.put(bucketName, new ZNRecord(bucketName));
        }
        ZNRecord bucketizedRecord = map.get(bucketName);
        bucketizedRecord.setMapField(partitionName, record.getMapField(partitionName));
      }
      else
      {
        LOG.error("Can't bucketize " + partitionName + " in map field");
      }
    }
    
    // copy all simple fields
    for (ZNRecord bucketizedRecord : map.values())
    {
      bucketizedRecord.setSimpleFields(record.getSimpleFields());
    }
    return map;
  }
}

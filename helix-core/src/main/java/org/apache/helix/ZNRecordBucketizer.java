package org.apache.helix;

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

import org.apache.log4j.Logger;

/**
 * Operations to divide a ZNRecord into specified buckets
 */
public class ZNRecordBucketizer {
  private static Logger LOG = Logger.getLogger(ZNRecordBucketizer.class);
  final int _bucketSize;

  /**
   * Instantiate a bucketizer with the number of buckets
   * @param bucketSize
   */
  public ZNRecordBucketizer(int bucketSize) {
    if (bucketSize <= 0) {
      LOG.debug("bucketSize <= 0 (was " + bucketSize
          + "). Set to 0 to use non-bucketized HelixProperty.");
      bucketSize = 0;
    }

    _bucketSize = bucketSize;
  }

  /**
   * Calculate bucketName in form of "resourceName_p{startPartition}-p{endPartition}
   * @param partitionName
   * @return the bucket name
   */
  public String getBucketName(String key) {
    if (_bucketSize == 0) {
      // no bucketize
      return null;
    }

    int idx = key.lastIndexOf('_');
    if (idx < 0) {
      throw new IllegalArgumentException("Could NOT find partition# in " + key
          + ". partitionName should be in format of resourceName_partition#");
    }

    try {
      int partitionNb = Integer.parseInt(key.substring(idx + 1));
      int bucketNb = partitionNb / _bucketSize;
      int startPartition = bucketNb * _bucketSize;
      int endPartition = bucketNb * _bucketSize + (_bucketSize - 1);
      return key.substring(0, idx) + "_p" + startPartition + "-p" + endPartition;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Could NOT parse partition# (" + key.substring(idx + 1)
          + ") in " + key);
    }
  }

  /**
   * Bucketize a ZNRecord
   * @param record
   * @return A map of bucket names to bucketized records
   */
  public Map<String, ZNRecord> bucketize(ZNRecord record) {
    Map<String, ZNRecord> map = new HashMap<String, ZNRecord>();
    if (_bucketSize == 0) {
      map.put(record.getId(), record);
      return map;
    }

    // bucketize list field
    for (String partitionName : record.getListFields().keySet()) {
      String bucketName = getBucketName(partitionName);
      if (bucketName != null) {
        if (!map.containsKey(bucketName)) {
          map.put(bucketName, new ZNRecord(bucketName));
        }
        ZNRecord bucketizedRecord = map.get(bucketName);
        bucketizedRecord.setListField(partitionName, record.getListField(partitionName));
      } else {
        LOG.error("Can't bucketize " + partitionName + " in list field");
      }
    }

    // bucketize map field
    for (String partitionName : record.getMapFields().keySet()) {
      String bucketName = getBucketName(partitionName);
      if (bucketName != null) {
        if (!map.containsKey(bucketName)) {
          map.put(bucketName, new ZNRecord(bucketName));
        }
        ZNRecord bucketizedRecord = map.get(bucketName);
        bucketizedRecord.setMapField(partitionName, record.getMapField(partitionName));
      } else {
        LOG.error("Can't bucketize " + partitionName + " in map field");
      }
    }

    // copy all simple fields
    for (ZNRecord bucketizedRecord : map.values()) {
      bucketizedRecord.setSimpleFields(record.getSimpleFields());
    }
    return map;
  }
}

package org.apache.helix.util;

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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO find a proper place for these methods
public final class ZNRecordUtil {
  private static final Logger logger = LoggerFactory.getLogger(ZNRecordUtil.class.getName());

  private ZNRecordUtil() {
  }

  public static ZNRecord find(String id, List<ZNRecord> list) {
    for (ZNRecord record : list) {
      if (record.getId() != null && record.getId().equals(id)) {
        return record;
      }
    }
    return null;
  }

  public static Map<String, ZNRecord> convertListToMap(List<ZNRecord> recordList) {
    Map<String, ZNRecord> recordMap = new HashMap<String, ZNRecord>();
    for (ZNRecord record : recordList) {
      if (record.getId() != null) {
        recordMap.put(record.getId(), record);
      }
    }
    return recordMap;
  }

  public static <T extends Object> List<T> convertListToTypedList(List<ZNRecord> recordList,
      Class<T> clazz) {
    List<T> list = new ArrayList<T>();
    for (ZNRecord record : recordList) {
      if (record.getId() == null) {
        logger.error("Invalid record: Id missing in " + record);
        continue;
      }
      try {

        Constructor<T> constructor = clazz.getConstructor(new Class[] {
          ZNRecord.class
        });
        T instance = constructor.newInstance(record);
        list.add(instance);
      } catch (Exception e) {
        logger.error("Error creating an Object of type:" + clazz.getCanonicalName(), e);
      }
    }
    return list;
  }

  public static <T extends Object> Map<String, T> convertListToTypedMap(List<ZNRecord> recordList,
      Class<T> clazz) {
    Map<String, T> map = new HashMap<String, T>();
    for (ZNRecord record : recordList) {
      if (record.getId() == null) {
        logger.error("Invalid record: Id missing in " + record);
        continue;
      }
      try {

        Constructor<T> constructor = clazz.getConstructor(new Class[] {
          ZNRecord.class
        });
        T instance = constructor.newInstance(record);
        map.put(record.getId(), instance);
      } catch (Exception e) {
        logger.error("Error creating an Object of type:" + clazz.getCanonicalName(), e);
      }
    }
    return map;
  }

  public static <T extends Object> List<T> convertMapToList(Map<String, T> map) {
    List<T> list = new ArrayList<T>();
    for (T t : map.values()) {
      list.add(t);
    }
    return list;
  }

  /**
   * Checks whether or not a serialized ZNRecord bytes should be compressed before being written to
   * Zookeeper.
   *
   * @param record raw ZNRecord before being serialized
   * @param serializedLength length of the serialized bytes array
   * @return
   */
  public static boolean shouldCompress(ZNRecord record, int serializedLength) {
    if (record.getBooleanField(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD, false)) {
      return true;
    }

    boolean autoCompressEnabled = Boolean.parseBoolean(System
        .getProperty(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_ENABLED,
            ZNRecord.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_DEFAULT));

    return autoCompressEnabled && serializedLength > getSerializerCompressThreshold();
  }

  /**
   * Returns the threshold in bytes that ZNRecord serializer should compress a ZNRecord with larger size.
   * If threshold is configured to be less than or equal to 0, the serializer will always compress ZNRecords as long as
   * auto-compression is enabled.
   * If threshold is not configured or the threshold is larger than ZNRecord write size limit, the default value
   * ZNRecord write size limit will be used instead.
   */
  private static int getSerializerCompressThreshold() {
    Integer autoCompressThreshold =
        Integer.getInteger(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_AUTO_COMPRESS_THRESHOLD_BYTES);

    if (autoCompressThreshold == null || autoCompressThreshold > getSerializerWriteSizeLimit()) {
      return getSerializerWriteSizeLimit();
    }

    return autoCompressThreshold;
  }

  /**
   * Returns ZNRecord serializer write size limit in bytes. If size limit is configured to be less
   * than or equal to 0, the default value {@link ZNRecord#SIZE_LIMIT} will be used instead.
   */
  public static int getSerializerWriteSizeLimit() {
    Integer writeSizeLimit =
        Integer.getInteger(SystemPropertyKeys.ZK_SERIALIZER_ZNRECORD_WRITE_SIZE_LIMIT_BYTES);

    if (writeSizeLimit == null || writeSizeLimit <= 0) {
      return ZNRecord.SIZE_LIMIT;
    }

    return writeSizeLimit;
  }
}

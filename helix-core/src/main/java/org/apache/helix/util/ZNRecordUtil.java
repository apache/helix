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

import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;

//TODO find a proper place for these methods
public final class ZNRecordUtil {
  private static final Logger logger = Logger.getLogger(ZNRecordUtil.class.getName());

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
}

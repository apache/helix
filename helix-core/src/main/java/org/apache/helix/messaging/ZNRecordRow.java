package org.apache.helix.messaging;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ZNRecord;

/**
 * A Normalized form of ZNRecord
 */
public class ZNRecordRow {
  // "Field names" in the flattened ZNRecord
  public static final String SIMPLE_KEY = "simpleKey";
  public static final String SIMPLE_VALUE = "simpleValue";

  public static final String LIST_KEY = "listKey";
  public static final String LIST_VALUE = "listValue";
  public static final String LIST_VALUE_INDEX = "listValueIndex";

  public static final String MAP_KEY = "mapKey";
  public static final String MAP_SUBKEY = "mapSubKey";
  public static final String MAP_VALUE = "mapValue";
  public static final String ZNRECORD_ID = "recordId";
  // ZNRECORD path ?

  final Map<String, String> _rowDataMap = new HashMap<String, String>();

  public ZNRecordRow() {
    _rowDataMap.put(SIMPLE_KEY, "");
    _rowDataMap.put(SIMPLE_VALUE, "");
    _rowDataMap.put(LIST_KEY, "");
    _rowDataMap.put(LIST_VALUE, "");
    _rowDataMap.put(LIST_VALUE_INDEX, "");
    _rowDataMap.put(MAP_KEY, "");
    _rowDataMap.put(MAP_SUBKEY, "");
    _rowDataMap.put(MAP_VALUE, "");
    _rowDataMap.put(ZNRECORD_ID, "");
  }

  public String getField(String rowField) {
    return _rowDataMap.get(rowField);
  }

  public void putField(String fieldName, String fieldValue) {
    _rowDataMap.put(fieldName, fieldValue);
  }

  public String getListValueIndex() {
    return getField(LIST_VALUE_INDEX);
  }

  public String getSimpleKey() {
    return getField(SIMPLE_KEY);
  }

  public String getSimpleValue() {
    return getField(SIMPLE_VALUE);
  }

  public String getListKey() {
    return getField(LIST_KEY);
  }

  public String getListValue() {
    return getField(LIST_VALUE);
  }

  public String getMapKey() {
    return getField(MAP_KEY);
  }

  public String getMapSubKey() {
    return getField(MAP_SUBKEY);
  }

  public String getMapValue() {
    return getField(MAP_VALUE);
  }

  public String getRecordId() {
    return getField(ZNRECORD_ID);
  }

  /* Josql function handlers */
  public static String getField(ZNRecordRow row, String rowField) {
    return row.getField(rowField);
  }

  public static List<ZNRecordRow> convertSimpleFields(ZNRecord record) {
    List<ZNRecordRow> result = new ArrayList<ZNRecordRow>();
    for (String key : record.getSimpleFields().keySet()) {
      ZNRecordRow row = new ZNRecordRow();
      row.putField(ZNRECORD_ID, record.getId());
      row.putField(SIMPLE_KEY, key);
      row.putField(SIMPLE_VALUE, record.getSimpleField(key));
      result.add(row);
    }
    return result;
  }

  public static List<ZNRecordRow> convertListFields(ZNRecord record) {
    List<ZNRecordRow> result = new ArrayList<ZNRecordRow>();
    for (String key : record.getListFields().keySet()) {
      int order = 0;
      for (String value : record.getListField(key)) {
        ZNRecordRow row = new ZNRecordRow();
        row.putField(ZNRECORD_ID, record.getId());
        row.putField(LIST_KEY, key);
        row.putField(LIST_VALUE, record.getSimpleField(key));
        row.putField(LIST_VALUE_INDEX, "" + order);
        order++;
        result.add(row);
      }
    }
    return result;
  }

  public static List<ZNRecordRow> convertMapFields(ZNRecord record) {
    List<ZNRecordRow> result = new ArrayList<ZNRecordRow>();
    for (String key0 : record.getMapFields().keySet()) {
      for (String key1 : record.getMapField(key0).keySet()) {
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

  public static List<ZNRecordRow> flatten(ZNRecord record) {
    List<ZNRecordRow> result = convertMapFields(record);
    result.addAll(convertListFields(record));
    result.addAll(convertSimpleFields(record));
    return result;
  }

  public static List<ZNRecordRow> flatten(Collection<ZNRecord> recordList) {
    List<ZNRecordRow> result = new ArrayList<ZNRecordRow>();
    for (ZNRecord record : recordList) {
      result.addAll(flatten(record));
    }
    return result;
  }

  public static List<ZNRecordRow> getRowListFromMap(Map<String, List<ZNRecordRow>> rowMap,
      String key) {
    return rowMap.get(key);
  }

  @Override
  public String toString() {
    return _rowDataMap.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ZNRecordRow) {
      ZNRecordRow that = (ZNRecordRow) other;
      return this._rowDataMap.equals(that._rowDataMap);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return _rowDataMap.hashCode();
  }
}

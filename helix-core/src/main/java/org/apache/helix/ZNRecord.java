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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.ZNRecordDelta.MergeOperation;
import org.apache.helix.manager.zk.serializer.JacksonPayloadSerializer;
import org.apache.helix.manager.zk.serializer.PayloadSerializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

/**
 * Generic Record Format to store data at a Node This can be used to store
 * simpleFields mapFields listFields
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = Inclusion.NON_NULL)
public class ZNRecord {
  static Logger _logger = Logger.getLogger(ZNRecord.class);
  private final String id;

  @JsonIgnore(true)
  public static final String LIST_FIELD_BOUND = "listField.bound";

  @JsonIgnore(true)
  public static final int SIZE_LIMIT = 1000 * 1024; // leave a margin out of 1M

  // We don't want the _deltaList to be serialized and deserialized
  private List<ZNRecordDelta> _deltaList = new ArrayList<ZNRecordDelta>();

  private Map<String, String> simpleFields;
  private Map<String, Map<String, String>> mapFields;
  private Map<String, List<String>> listFields;
  private byte[] rawPayload;

  private PayloadSerializer _serializer;

  // the version field of zookeeper Stat
  private int _version;

  private long _creationTime;

  private long _modifiedTime;

  /**
   * Initialize with an identifier
   * @param id
   */
  @JsonCreator
  public ZNRecord(@JsonProperty("id") String id) {
    this.id = id;
    simpleFields = new TreeMap<String, String>();
    mapFields = new TreeMap<String, Map<String, String>>();
    listFields = new TreeMap<String, List<String>>();
    rawPayload = null;
    _serializer = new JacksonPayloadSerializer();
  }

  /**
   * Initialize with a pre-populated ZNRecord
   * @param record
   */
  public ZNRecord(ZNRecord record) {
    this(record, record.getId());
  }

  /**
   * Initialize with a pre-populated ZNRecord, overwriting the identifier
   * @param record
   * @param id
   */
  public ZNRecord(ZNRecord record, String id) {
    this(id);
    simpleFields.putAll(record.getSimpleFields());
    mapFields.putAll(record.getMapFields());
    listFields.putAll(record.getListFields());
    if (record.rawPayload != null) {
      rawPayload = new byte[record.rawPayload.length];
      System.arraycopy(record.rawPayload, 0, rawPayload, 0, record.rawPayload.length);
    } else {
      rawPayload = null;
    }
    _version = record.getVersion();
    _creationTime = record.getCreationTime();
    _modifiedTime = record.getModifiedTime();
  }

  /**
   * Set a custom {@link PayloadSerializer} to allow including arbitrary data
   * @param serializer
   */
  @JsonIgnore(true)
  public void setPayloadSerializer(PayloadSerializer serializer) {
    _serializer = serializer;
  }

  /**
   * Get the {@link PayloadSerializer} that will serialize/deserialize the payload
   * @return serializer
   */
  @JsonIgnore(true)
  public PayloadSerializer getPayloadSerializer() {
    return _serializer;
  }

  /**
   * Set the list of updates to this ZNRecord
   * @param deltaList
   */
  @JsonIgnore(true)
  public void setDeltaList(List<ZNRecordDelta> deltaList) {
    _deltaList = deltaList;
  }

  /**
   * Get the list of updates to this ZNRecord
   * @return list of {@link ZNRecordDelta}
   */
  @JsonIgnore(true)
  public List<ZNRecordDelta> getDeltaList() {
    return _deltaList;
  }

  /**
   * Get all plain key, value fields
   * @return Map of simple fields
   */
  @JsonProperty
  public Map<String, String> getSimpleFields() {
    return simpleFields;
  }

  /**
   * Set all plain key, value fields
   * @param simpleFields
   */
  @JsonProperty
  public void setSimpleFields(Map<String, String> simpleFields) {
    this.simpleFields = simpleFields;
  }

  /**
   * Get all fields whose values are key, value properties
   * @return all map fields
   */
  @JsonProperty
  public Map<String, Map<String, String>> getMapFields() {
    return mapFields;
  }

  /**
   * Set all fields whose values are key, value properties
   * @param mapFields
   */
  @JsonProperty
  public void setMapFields(Map<String, Map<String, String>> mapFields) {
    this.mapFields = mapFields;
  }

  /**
   * Get all fields whose values are a list of values
   * @return all list fields
   */
  @JsonProperty
  public Map<String, List<String>> getListFields() {
    return listFields;
  }

  /**
   * Set all fields whose values are a list of values
   * @param listFields
   */
  @JsonProperty
  public void setListFields(Map<String, List<String>> listFields) {
    this.listFields = listFields;
  }

  /**
   * Set a simple key, value field
   * @param k
   * @param v
   */
  @JsonProperty
  public void setSimpleField(String k, String v) {
    simpleFields.put(k, v);
  }

  @JsonProperty
  public String getId() {
    return id;
  }

  /**
   * Set arbitrary data serialized as a byte array payload. Consider using
   * {@link #setPayload(Object)} instead
   * @param payload
   */
  @JsonProperty
  public void setRawPayload(byte[] payload) {
    rawPayload = payload;
  }

  /**
   * Get arbitrary data serialized as a byte array payload. Consider using
   * {@link #getPayload(Class)} instead
   * @return
   */
  @JsonProperty
  public byte[] getRawPayload() {
    return rawPayload;
  }

  /**
   * Set a typed payload that will be serialized and persisted.
   * @param payload
   */
  @JsonIgnore(true)
  public <T> void setPayload(T payload) {
    if (_serializer != null && payload != null) {
      rawPayload = _serializer.serialize(payload);
    } else {
      rawPayload = null;
    }
  }

  /**
   * Get a typed deserialized payload
   * @param clazz
   * @return
   */
  @JsonIgnore(true)
  public <T> T getPayload(Class<T> clazz) {
    if (_serializer != null && rawPayload != null) {
      return _serializer.deserialize(clazz, rawPayload);
    } else {
      return null;
    }
  }

  /**
   * Set a single String --> Map field
   * @param k
   * @param v
   */
  public void setMapField(String k, Map<String, String> v) {
    mapFields.put(k, v);
  }

  /**
   * Set a single String --> List field
   * @param k
   * @param v
   */
  public void setListField(String k, List<String> v) {
    listFields.put(k, v);
  }

  /**
   * Get a single String field
   * @param k
   * @return String field
   */
  public String getSimpleField(String k) {
    return simpleFields.get(k);
  }

  /**
   * Get a single Map field
   * @param k
   * @return String --> String map
   */
  public Map<String, String> getMapField(String k) {
    return mapFields.get(k);
  }

  /**
   * Get a single List field
   * @param k
   * @return String list
   */
  public List<String> getListField(String k) {
    return listFields.get(k);
  }

  /**
   * Set a single simple int field
   * @param k
   * @param v
   */
  public void setIntField(String k, int v) {
    setSimpleField(k, Integer.toString(v));
  }

  /**
   * Get a single int field
   * @param k
   * @param defaultValue
   * @return the int value, or defaultValue if not present
   */
  public int getIntField(String k, int defaultValue) {
    int v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null) {
      try {
        v = Integer.parseInt(valueStr);
      } catch (NumberFormatException e) {
        _logger.warn("", e);
      }
    }
    return v;
  }

  /**
   * Set a single simple long field
   * @param k
   * @param v
   */
  public void setLongField(String k, long v) {
    setSimpleField(k, Long.toString(v));
  }

  /**
   * Get a single long field
   * @param k
   * @param defaultValue
   * @return the long value, or defaultValue if not present
   */
  public long getLongField(String k, long defaultValue) {
    long v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null) {
      try {
        v = Long.parseLong(valueStr);
      } catch (NumberFormatException e) {
        _logger.warn("", e);
      }
    }
    return v;
  }

  /**
   * Set a single simple double field
   * @param k
   * @param v
   */
  public void setDoubleField(String k, double v) {
    setSimpleField(k, Double.toString(v));
  }

  /**
   * Get a single double field
   * @param k
   * @param defaultValue
   * @return the double value, or defaultValue if not present
   */
  public double getDoubleField(String k, double defaultValue) {
    double v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null) {
      try {
        v = Double.parseDouble(valueStr);
      } catch (NumberFormatException e) {
        _logger.warn("", e);
      }
    }
    return v;
  }

  /**
   * Set a single simple boolean field
   * @param k
   * @param v
   */
  public void setBooleanField(String k, boolean v) {
    setSimpleField(k, Boolean.toString(v));
  }

  /**
   * Get a single boolean field
   * @param k
   * @param defaultValue
   * @return the boolean field, or defaultValue if not present
   */
  public boolean getBooleanField(String k, boolean defaultValue) {
    boolean v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null) {
      // Boolean.parseBoolean() doesn't throw an exception if the string isn't a valid boolean.
      // Thus, a direct comparison is necessary to make sure the value is actually "true" or
      // "false"
      if (valueStr.equalsIgnoreCase(Boolean.TRUE.toString())) {
        v = true;
      } else if (valueStr.equalsIgnoreCase(Boolean.FALSE.toString())) {
        v = false;
      }
    }
    return v;
  }

  /**
   * Set a single simple Enum field
   * @param k
   * @param v
   */
  public <T extends Enum<T>> void setEnumField(String k, T v) {
    setSimpleField(k, v.toString());
  }

  /**
   * Get a single Enum field
   * @param k
   * @param enumType
   * @param defaultValue
   * @return the Enum field of enumType, or defaultValue if not present
   */
  public <T extends Enum<T>> T getEnumField(String k, Class<T> enumType, T defaultValue) {
    T v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null) {
      try {
        v = Enum.valueOf(enumType, valueStr);
      } catch (NullPointerException e) {
        _logger.warn("", e);
      } catch (IllegalArgumentException e) {
        _logger.warn("", e);
      }
    }
    return v;
  }

  /**
   * Get a single String field
   * @param k
   * @param defaultValue
   * @return the String value, or defaultValue if not present
   */
  public String getStringField(String k, String defaultValue) {
    String v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null) {
      v = valueStr;
    }
    return v;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(id + ", ");
    if (simpleFields != null) {
      sb.append(simpleFields);
    }
    if (mapFields != null) {
      sb.append(mapFields);
    }
    if (listFields != null) {
      sb.append(listFields);
    }
    return sb.toString();
  }

  /**
   * merge functionality is used to merge multiple znrecord into a single one.
   * This will make use of the id of each ZNRecord and append it to every key
   * thus making key unique. This is needed to optimize on the watches.
   * @param record
   */
  public void merge(ZNRecord record) {
    if (record == null) {
      return;
    }

    if (record.getDeltaList().size() > 0) {
      _logger.info("Merging with delta list, recordId = " + id + " other:" + record.getId());
      merge(record.getDeltaList());
      return;
    }
    simpleFields.putAll(record.simpleFields);
    for (String key : record.mapFields.keySet()) {
      Map<String, String> map = mapFields.get(key);
      if (map != null) {
        map.putAll(record.mapFields.get(key));
      } else {
        mapFields.put(key, record.mapFields.get(key));
      }
    }
    for (String key : record.listFields.keySet()) {
      List<String> list = listFields.get(key);
      if (list != null) {
        list.addAll(record.listFields.get(key));
      } else {
        listFields.put(key, record.listFields.get(key));
      }
    }
  }

  /**
   * Merge in a {@link ZNRecordDelta} corresponding to its merge policy
   * @param delta
   */
  void merge(ZNRecordDelta delta) {
    if (delta.getMergeOperation() == MergeOperation.ADD) {
      merge(delta.getRecord());
    } else if (delta.getMergeOperation() == MergeOperation.SUBTRACT) {
      subtract(delta.getRecord());
    }
  }

  /**
   * Batch merge of {@link ZNRecordDelta}
   * @see #merge(ZNRecordDelta)
   * @param deltaList
   */
  void merge(List<ZNRecordDelta> deltaList) {
    for (ZNRecordDelta delta : deltaList) {
      merge(delta);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ZNRecord)) {
      return false;
    }
    ZNRecord that = (ZNRecord) obj;
    if (this.getSimpleFields().size() != that.getSimpleFields().size()) {
      return false;
    }
    if (this.getMapFields().size() != that.getMapFields().size()) {
      return false;
    }
    if (this.getListFields().size() != that.getListFields().size()) {
      return false;
    }
    if (!this.getSimpleFields().equals(that.getSimpleFields())) {
      return false;
    }
    if (!this.getMapFields().equals(that.getMapFields())) {
      return false;
    }
    if (!this.getListFields().equals(that.getListFields())) {
      return false;
    }

    return true;
  }

  /**
   * Subtract value from this ZNRecord
   * Note: does not support subtract in each list in list fields or map in
   * mapFields
   * @param value
   */
  public void subtract(ZNRecord value) {
    for (String key : value.getSimpleFields().keySet()) {
      if (simpleFields.containsKey(key)) {
        simpleFields.remove(key);
      }
    }

    for (String key : value.getListFields().keySet()) {
      if (listFields.containsKey(key)) {
        listFields.remove(key);
      }
    }

    for (String key : value.getMapFields().keySet()) {
      Map<String, String> map = value.getMapField(key);
      if (map == null) {
        mapFields.remove(key);
      } else {
        Map<String, String> nestedMap = mapFields.get(key);
        if (nestedMap != null) {
          for (String mapKey : map.keySet()) {
            nestedMap.remove(mapKey);
          }
          if (nestedMap.size() == 0) {
            mapFields.remove(key);
          }
        }
      }
    }
  }

  /**
   * Get the version of this record
   * @return version number
   */
  @JsonIgnore(true)
  public int getVersion() {
    return _version;
  }

  /**
   * Set the version of this record
   * @param version
   */
  @JsonIgnore(true)
  public void setVersion(int version) {
    _version = version;
  }

  /**
   * Get the time that this record was created
   * @return UNIX timestamp
   */
  @JsonIgnore(true)
  public long getCreationTime() {
    return _creationTime;
  }

  /**
   * Set the time that this record was created
   * @param creationTime
   */
  @JsonIgnore(true)
  public void setCreationTime(long creationTime) {
    _creationTime = creationTime;
  }

  /**
   * Get the time that this record was last modified
   * @return UNIX timestamp
   */
  @JsonIgnore(true)
  public long getModifiedTime() {
    return _modifiedTime;
  }

  /**
   * Set the time that this record was last modified
   * @param modifiedTime
   */
  @JsonIgnore(true)
  public void setModifiedTime(long modifiedTime) {
    _modifiedTime = modifiedTime;
  }
}

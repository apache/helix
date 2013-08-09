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


/**
 * Generic Record Format to store data at a Node This can be used to store
 * simpleFields mapFields listFields
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZNRecord
{
  static Logger _logger = Logger.getLogger(ZNRecord.class);
  private final String id;

  @JsonIgnore(true)
  public static final String LIST_FIELD_BOUND = "listField.bound";

  @JsonIgnore(true)
  public static final int SIZE_LIMIT = 1000 * 1024;  // leave a margin out of 1M

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

  @JsonCreator
  public ZNRecord(@JsonProperty("id") String id)
  {
    this.id = id;
    simpleFields = new TreeMap<String, String>();
    mapFields = new TreeMap<String, Map<String, String>>();
    listFields = new TreeMap<String, List<String>>();
    rawPayload = new byte[0];
    _serializer = new JacksonPayloadSerializer();
  }

  public ZNRecord(ZNRecord record)
  {
    this(record, record.getId());
  }

  public ZNRecord(ZNRecord record, String id)
  {
    this(id);
    simpleFields.putAll(record.getSimpleFields());
    mapFields.putAll(record.getMapFields());
    listFields.putAll(record.getListFields());
    rawPayload = new byte[record.rawPayload.length];
    System.arraycopy(record.rawPayload, 0, rawPayload, 0, record.rawPayload.length);
    _version = record.getVersion();
    _creationTime = record.getCreationTime();
    _modifiedTime = record.getModifiedTime();
  }

  @JsonIgnore(true)
  public void setPayloadSerializer(PayloadSerializer serializer)
  {
    _serializer = serializer;
  }

  @JsonIgnore(true)
  public void setDeltaList(List<ZNRecordDelta> deltaList)
  {
    _deltaList = deltaList;
  }

  @JsonIgnore(true)
  public List<ZNRecordDelta> getDeltaList()
  {
    return _deltaList;
  }

  @JsonProperty
  public Map<String, String> getSimpleFields()
  {
    return simpleFields;
  }

  @JsonProperty
  public void setSimpleFields(Map<String, String> simpleFields)
  {
    this.simpleFields = simpleFields;
  }

  @JsonProperty
  public Map<String, Map<String, String>> getMapFields()
  {
    return mapFields;
  }

  @JsonProperty
  public void setMapFields(Map<String, Map<String, String>> mapFields)
  {
    this.mapFields = mapFields;
  }

  @JsonProperty
  public Map<String, List<String>> getListFields()
  {
    return listFields;
  }

  @JsonProperty
  public void setListFields(Map<String, List<String>> listFields)
  {
    this.listFields = listFields;
  }

  @JsonProperty
  public void setSimpleField(String k, String v)
  {
    simpleFields.put(k, v);
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public void setRawPayload(byte[] payload)
  {
    rawPayload = payload;
  }

  @JsonProperty
  public byte[] getRawPayload()
  {
    return rawPayload;
  }

  @JsonIgnore(true)
  public <T> void setPayload(T payload)
  {
    if (_serializer != null && payload != null)
    {
      rawPayload = _serializer.serialize(payload);
    }
    else
    {
      rawPayload = null;
    }
  }

  @JsonIgnore(true)
  public <T> T getPayload(Class<T> clazz)
  {
    if (_serializer != null && rawPayload != null)
    {
      return _serializer.deserialize(clazz, rawPayload);
    }
    else
    {
      return null;
    }
  }

  public void setMapField(String k, Map<String, String> v)
  {
    mapFields.put(k, v);
  }

  public void setListField(String k, List<String> v)
  {
    listFields.put(k, v);
  }

  public String getSimpleField(String k)
  {
    return simpleFields.get(k);
  }

  public Map<String, String> getMapField(String k)
  {
    return mapFields.get(k);
  }

  public List<String> getListField(String k)
  {
    return listFields.get(k);
  }

  public void setIntField(String k, int v)
  {
    setSimpleField(k, Integer.toString(v));
  }

  public int getIntField(String k, int defaultValue)
  {
    int v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null)
    {
      try
      {
        v = Integer.parseInt(valueStr);
      }
      catch (NumberFormatException e)
      {
        _logger.warn("", e);
      }
    }
    return v;
  }

  public void setLongField(String k, long v)
  {
    setSimpleField(k, Long.toString(v));
  }

  public long getLongField(String k, long defaultValue)
  {
    long v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null)
    {
      try
      {
        v = Long.parseLong(valueStr);
      }
      catch (NumberFormatException e)
      {
        _logger.warn("", e);
      }
    }
    return v;
  }

  public void setDoubleField(String k, double v)
  {
    setSimpleField(k, Double.toString(v));
  }

  public double getDoubleField(String k, double defaultValue)
  {
    double v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null)
    {
      try
      {
        v = Double.parseDouble(valueStr);
      }
      catch (NumberFormatException e)
      {
        _logger.warn("", e);
      }
    }
    return v;
  }

  public void setBooleanField(String k, boolean v)
  {
    setSimpleField(k, Boolean.toString(v));
  }

  public boolean getBooleanField(String k, boolean defaultValue)
  {
    boolean v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null)
    {
      // Boolean.parseBoolean() doesn't throw an exception if the string isn't a valid boolean.
      // Thus, a direct comparison is necessary to make sure the value is actually "true" or
      // "false"
      if (valueStr.equalsIgnoreCase(Boolean.TRUE.toString()))
      {
        v = true;
      }
      else if (valueStr.equalsIgnoreCase(Boolean.FALSE.toString()))
      {
        v = false;
      }
    }
    return v;
  }

  public <T extends Enum<T>> void setEnumField(String k, T v)
  {
    setSimpleField(k, v.toString());
  }

  public <T extends Enum<T>> T getEnumField(String k, Class<T> enumType, T defaultValue)
  {
    T v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null)
    {
      try
      {
        v = Enum.valueOf(enumType, valueStr);
      }
      catch (NullPointerException e)
      {
        _logger.warn("", e);
      }
      catch (IllegalArgumentException e)
      {
        _logger.warn("", e);
      }
    }
    return v;
  }

  public String getStringField(String k, String defaultValue)
  {
    String v = defaultValue;
    String valueStr = getSimpleField(k);
    if (valueStr != null)
    {
      v = valueStr;
    }
    return v;
  }

  @Override
  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    sb.append(id + ", ");
    if (simpleFields != null)
    {
      sb.append(simpleFields);
    }
    if (mapFields != null)
    {
      sb.append(mapFields);
    }
    if (listFields != null)
    {
      sb.append(listFields);
    }
    return sb.toString();
  }

  /**
   * merge functionality is used to merge multiple znrecord into a single one.
   * This will make use of the id of each ZNRecord and append it to every key
   * thus making key unique. This is needed to optimize on the watches.
   *
   * @param record
   */
  public void merge(ZNRecord record)
  {
    if (record == null)
    {
      return;
    }

    if (record.getDeltaList().size() > 0)
    {
      _logger.info("Merging with delta list, recordId = " + id + " other:"
          + record.getId());
      merge(record.getDeltaList());
      return;
    }
    simpleFields.putAll(record.simpleFields);
    for (String key : record.mapFields.keySet())
    {
      Map<String, String> map = mapFields.get(key);
      if (map != null)
      {
        map.putAll(record.mapFields.get(key));
      } else
      {
        mapFields.put(key, record.mapFields.get(key));
      }
    }
    for (String key : record.listFields.keySet())
    {
      List<String> list = listFields.get(key);
      if (list != null)
      {
        list.addAll(record.listFields.get(key));
      } else
      {
        listFields.put(key, record.listFields.get(key));
      }
    }
  }

  void merge(ZNRecordDelta delta)
  {
    if (delta.getMergeOperation() == MergeOperation.ADD)
    {
      merge(delta.getRecord());
    } else if (delta.getMergeOperation() == MergeOperation.SUBTRACT)
    {
      subtract(delta.getRecord());
    }
  }

  void merge(List<ZNRecordDelta> deltaList)
  {
    for (ZNRecordDelta delta : deltaList)
    {
      merge(delta);
    }
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof ZNRecord))
    {
      return false;
    }
    ZNRecord that = (ZNRecord) obj;
    if (this.getSimpleFields().size() != that.getSimpleFields().size())
    {
      return false;
    }
    if (this.getMapFields().size() != that.getMapFields().size())
    {
      return false;
    }
    if (this.getListFields().size() != that.getListFields().size())
    {
      return false;
    }
    if (!this.getSimpleFields().equals(that.getSimpleFields()))
    {
      return false;
    }
    if (!this.getMapFields().equals(that.getMapFields()))
    {
      return false;
    }
    if (!this.getListFields().equals(that.getListFields()))
    {
      return false;
    }

    return true;
  }

  /**
   * Note: does not support subtract in each list in list fields or map in
   * mapFields
   */
  public void subtract(ZNRecord value)
  {
    for (String key : value.getSimpleFields().keySet())
    {
      if (simpleFields.containsKey(key))
      {
        simpleFields.remove(key);
      }
    }

    for (String key : value.getListFields().keySet())
    {
      if (listFields.containsKey(key))
      {
        listFields.remove(key);
      }
    }

    for (String key : value.getMapFields().keySet())
    {
      if (mapFields.containsKey(key))
      {
        mapFields.remove(key);
      }
    }
  }

  @JsonIgnore(true)
  public int getVersion()
  {
    return _version;
  }

  @JsonIgnore(true)
  public void setVersion(int version)
  {
    _version = version;
  }

  @JsonIgnore(true)
  public long getCreationTime()
  {
    return _creationTime;
  }

  @JsonIgnore(true)
  public void setCreationTime(long creationTime)
  {
    _creationTime = creationTime;
  }

  @JsonIgnore(true)
  public long getModifiedTime()
  {
    return _modifiedTime;
  }

  @JsonIgnore(true)
  public void setModifiedTime(long modifiedTime)
  {
    _modifiedTime = modifiedTime;
  }
}

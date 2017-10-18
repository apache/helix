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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class for ZNRecord. Used as a base class for IdealState, CurrentState, etc.
 */
public class HelixProperty {
  private static Logger LOG = LoggerFactory.getLogger(HelixProperty.class);

  public enum HelixPropertyAttribute {
    BUCKET_SIZE,
    BATCH_MESSAGE_MODE
  }

  protected final ZNRecord _record;

  /**
   * Metadata of a HelixProperty
   */
  public static class Stat {
    // the version field of zookeeper Stat
    private int _version;
    private long _creationTime;
    private long _modifiedTime;

    public Stat(int version, long creationTime, long modifiedTime) {
      _version = version;
      _creationTime = creationTime;
      _modifiedTime = modifiedTime;
    }

    public Stat(Stat stat) {
      _version = stat.getVersion();
      _creationTime = stat.getCreationTime();
      _modifiedTime = stat.getModifiedTime();
    }

    public Stat() {
      _version = -1;
      _creationTime = -1;
      _modifiedTime = -1;
    }

    public int getVersion() {
      return _version;
    }

    public void setVersion(int version) {
      _version = version;
    }

    public long getCreationTime() {
      return _creationTime;
    }

    public void setCreationTime(long creationTime) {
      _creationTime = creationTime;
    }

    public long getModifiedTime() {
      return _modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
      _modifiedTime = modifiedTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Stat)) {
        return false;
      }

      Stat stat = (Stat) o;

      if (_version != stat._version) {
        return false;
      }
      if (_creationTime != stat._creationTime) {
        return false;
      }
      return _modifiedTime == stat._modifiedTime;
    }

    @Override
    public int hashCode() {
      int result = _version;
      result = 31 * result + (int) (_creationTime ^ (_creationTime >>> 32));
      result = 31 * result + (int) (_modifiedTime ^ (_modifiedTime >>> 32));
      return result;
    }
  }

  private Stat _stat;

  /**
   * Initialize the property with an identifier
   * @param id
   */
  public HelixProperty(String id) {
    this(new ZNRecord(id), id);
  }

  /**
   * Initialize the property with an existing ZNRecord
   * @param record
   */
  public HelixProperty(ZNRecord record) {
    this(record, record.getId());
  }

  /**
   * Initialize the property with an existing ZNRecord with new record id
   * @param record
   * @param id
   */
  public HelixProperty(ZNRecord record, String id) {
    _record = new ZNRecord(record, id);
    _stat = new Stat(_record.getVersion(), _record.getCreationTime(), _record.getModifiedTime());
  }

  /**
   * Get the property identifier
   * @return the property id
   */
  public final String getId() {
    return _record.getId();
  }

  /**
   * Get the backing ZNRecord
   * @return ZNRecord object associated with this property
   */
  public final ZNRecord getRecord() {
    return _record;
  }

  /**
   * Set the changes to the backing ZNRecord
   * @param deltaList list of ZNRecord updates to be made
   */
  public final void setDeltaList(List<ZNRecordDelta> deltaList) {
    _record.setDeltaList(deltaList);
  }

  @Override
  public String toString() {
    return _record.toString();
  }

  /**
   * Get the size of buckets defined
   * @return the bucket size, or 0 if not defined
   */
  public int getBucketSize() {
    String bucketSizeStr = _record.getSimpleField(HelixPropertyAttribute.BUCKET_SIZE.toString());
    int bucketSize = 0;
    if (bucketSizeStr != null) {
      try {
        bucketSize = Integer.parseInt(bucketSizeStr);
      } catch (NumberFormatException e) {
        // OK
      }
    }
    return bucketSize;
  }

  /**
   * Set the size of buckets defined
   * @param bucketSize the bucket size (will default to 0 if negative)
   */
  public void setBucketSize(int bucketSize) {
    if (bucketSize <= 0)
      bucketSize = 0;

    _record.setSimpleField(HelixPropertyAttribute.BUCKET_SIZE.toString(), "" + bucketSize);
  }

  /**
   * static method that converts ZNRecord to an instance that subclasses HelixProperty
   * @param clazz subclass of HelixProperty
   * @param record the ZNRecord describing the property
   * @return typed instance corresponding to the record, or null if conversion fails
   */
  public static <T extends HelixProperty> T convertToTypedInstance(Class<T> clazz, ZNRecord record) {
    if (record == null) {
      return null;
    }

    try {
      Constructor<T> constructor = clazz.getConstructor(ZNRecord.class);
      return constructor.newInstance(record);
    } catch (Exception e) {
      LOG.error("Exception convert znrecord: " + record + " to class: " + clazz, e);
    }

    return null;
  }

  /**
   * Convert a collection of records to typed properties
   * @param clazz Subclass of HelixProperty
   * @param records the ZNRecords describing the property
   * @return list of typed instances for which the conversion succeeded, or null if records is null
   */
  public static <T extends HelixProperty> List<T> convertToTypedList(Class<T> clazz,
      Collection<ZNRecord> records) {
    if (records == null) {
      return null;
    }

    List<T> decorators = new ArrayList<T>();
    for (ZNRecord record : records) {
      T decorator = HelixProperty.convertToTypedInstance(clazz, record);
      if (decorator != null) {
        decorators.add(decorator);
      }
    }
    return decorators;
  }

  /**
   * Converts a list of records to a map of the record identifier to typed properties
   * @param records the ZNRecords to convert
   * @return id --> HelixProperty subclass map
   */
  public static <T extends HelixProperty> Map<String, T> convertListToMap(List<T> records) {
    if (records == null) {
      return Collections.emptyMap();
    }

    Map<String, T> decorators = new HashMap<String, T>();
    for (T record : records) {
      decorators.put(record.getId(), record);
    }
    return decorators;
  }

  /**
   * Convert typed properties to a list of records
   * @param typedInstances objects subclassing HelixProperty
   * @return a list of ZNRecord objects
   */
  public static <T extends HelixProperty> List<ZNRecord> convertToList(List<T> typedInstances) {
    if (typedInstances == null) {
      return Collections.emptyList();
    }

    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (T typedInstance : typedInstances) {
      records.add(typedInstance.getRecord());
    }

    return records;
  }

  /**
   * Change the state of batch messaging
   * @param enable true to enable, false to disable
   */
  public void setBatchMessageMode(boolean enable) {
    _record.setSimpleField(HelixPropertyAttribute.BATCH_MESSAGE_MODE.toString(), "" + enable);
  }

  /**
   * Get the state of batch messaging
   * @return true if enabled, false if disabled
   */
  public boolean getBatchMessageMode() {
    String enableStr = _record.getSimpleField(HelixPropertyAttribute.BATCH_MESSAGE_MODE.toString());
    if (enableStr == null) {
      return false;
    }

    try {
      return Boolean.parseBoolean(enableStr.toLowerCase());
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Get the metadata (stat) of this record
   * @return HelixProperty.Stat
   */
  public Stat getStat() {
    return _stat;
  }

  /**
   * Set the metadata (stat) for this record
   * @param stat
   */
  public void setStat(Stat stat) {
    _stat = new Stat(stat);
  }

  /**
   * Get property validity
   * @return true if valid, false if invalid
   */
  public boolean isValid() {
    return true;
  }

  public byte[] serialize(ZkSerializer serializer) {
    return serializer.serialize(_record);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof HelixProperty) {
      HelixProperty that = (HelixProperty) obj;
      if (that.getRecord() != null) {
        return that.getRecord().equals(this.getRecord());
      }
    }
    return false;
  }
}

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

/**
 * A wrapper class for ZNRecord. Used as a base class for IdealState, CurrentState, etc.
 */
public class HelixProperty
{
  public enum HelixPropertyAttribute
  {
    BUCKET_SIZE,
    GROUP_MESSAGE_MODE
  }

  protected final ZNRecord _record;

  public HelixProperty(String id)
  {
    _record = new ZNRecord(id);
  }

  public HelixProperty(ZNRecord record)
  {
    _record = new ZNRecord(record);
  }

  public final String getId()
  {
    return _record.getId();
  }

  public final ZNRecord getRecord()
  {
    return _record;
  }

  public final void setDeltaList(List<ZNRecordDelta> deltaList)
  {
    _record.setDeltaList(deltaList);
  }

  @Override
  public String toString()
  {
    return _record.toString();
  }

  public int getBucketSize()
  {
    String bucketSizeStr =
        _record.getSimpleField(HelixPropertyAttribute.BUCKET_SIZE.toString());
    int bucketSize = 0;
    if (bucketSizeStr != null)
    {
      try
      {
        bucketSize = Integer.parseInt(bucketSizeStr);
      }
      catch (NumberFormatException e)
      {
        // OK
      }
    }
    return bucketSize;
  }

  public void setBucketSize(int bucketSize)
  {
    if (bucketSize <= 0)
      bucketSize = 0;

    _record.setSimpleField(HelixPropertyAttribute.BUCKET_SIZE.toString(), "" + bucketSize);
  }

  /**
   * static method that convert ZNRecord to an instance that subclasses HelixProperty
   * 
   * @param clazz
   * @param record
   * @return
   */
  public static <T extends HelixProperty> T convertToTypedInstance(Class<T> clazz,
                                                                   ZNRecord record)
  {
    if (record == null)
    {
      return null;
    }

    try
    {
      Constructor<T> constructor = clazz.getConstructor(new Class[] { ZNRecord.class });
      return constructor.newInstance(record);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return null;
  }

  public static <T extends HelixProperty> List<T> convertToTypedList(Class<T> clazz,
                                                                     Collection<ZNRecord> records)
  {
    if (records == null)
    {
      return null;
    }

    List<T> decorators = new ArrayList<T>();
    for (ZNRecord record : records)
    {
      T decorator = HelixProperty.convertToTypedInstance(clazz, record);
      if (decorator != null)
      {
        decorators.add(decorator);
      }
    }
    return decorators;
  }

  public static <T extends HelixProperty> Map<String, T> convertListToMap(List<T> records)
  {
    if (records == null)
    {
      return Collections.emptyMap();
    }

    Map<String, T> decorators = new HashMap<String, T>();
    for (T record : records)
    {
      decorators.put(record.getId(), record);
    }
    return decorators;
  }

  public static <T extends HelixProperty> List<ZNRecord> convertToList(List<T> typedInstances)
  {
    if (typedInstances == null)
    {
      return Collections.emptyList();
    }

    List<ZNRecord> records = new ArrayList<ZNRecord>();
    for (T typedInstance : typedInstances)
    {
      records.add(typedInstance.getRecord());
    }

    return records;
  }

  public void setGroupMessageMode(boolean enable)
  {
    _record.setSimpleField(HelixPropertyAttribute.GROUP_MESSAGE_MODE.toString(), ""
        + enable);
  }

  public boolean getGroupMessageMode()
  {
    String enableStr =
        _record.getSimpleField(HelixPropertyAttribute.GROUP_MESSAGE_MODE.toString());
    if (enableStr == null)
    {
      return false;
    }

    try
    {
      return Boolean.parseBoolean(enableStr.toLowerCase());
    }
    catch (Exception e)
    {
      return false;
    }
  }
  
  public boolean isValid()
  {
    return true;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
    {
      return false;
    }
    if (obj instanceof HelixProperty)
    {
      HelixProperty that = (HelixProperty) obj;
      if (that.getRecord() != null)
      {
        return that.getRecord().equals(this.getRecord());
      }
    }
    return false;
  }
}

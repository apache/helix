/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A wrapper class for ZNRecord. Used as a parent class for IdealState,
 * CurrentState, etc.
 */
public abstract class ZNRecordDecorator
{
  protected final ZNRecord _record;

  public ZNRecordDecorator(String id)
  {
    _record = new ZNRecord(id);
  }

  public ZNRecordDecorator(ZNRecord record)
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

  /**
   * static method that convert ZNRecord to an instance that subclasses
   * ZNRecordDecorator
   * 
   * @param clazz
   * @param record
   * @return
   */
  public static <T extends ZNRecordDecorator> T convertToTypedInstance(
      Class<T> clazz, ZNRecord record)
  {
    if (record == null)
    {
      return null;
    }

    try
    {
      Constructor<T> constructor = clazz.getConstructor(new Class[]
      { ZNRecord.class });
      return constructor.newInstance(record);
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return null;
  }

  public static <T extends ZNRecordDecorator> List<T> convertToTypedList(
      Class<T> clazz, Collection<ZNRecord> records)
  {
    if (records == null)
    {
      return null;
    }

    List<T> decorators = new ArrayList<T>();
    for (ZNRecord record : records)
    {
      T decorator = ZNRecordDecorator.convertToTypedInstance(clazz, record);
      if (decorator != null)
      {
        decorators.add(decorator);
      }
    }
    return decorators;
  }

  public static <T extends ZNRecordDecorator> Map<String, T> convertListToMap(
      List<T> records)
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

  public abstract boolean isValid();

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
    {
      return false;
    }
    if (obj instanceof ZNRecordDecorator)
    {
      ZNRecordDecorator that = (ZNRecordDecorator) obj;
      if (that.getRecord() != null)
      {
        return that.getRecord().equals(this.getRecord());
      }
    }
    return false;
  }
}

package com.linkedin.helix;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A wrapper class for ZNRecord.
 * Used as a parent class for IdealState, CurrentState, etc.
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

  public final int getVersion()
  {
    return _record.getVersion();
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
   * static method that convert ZNRecord to an instance that subclasses ZNRecordDecorator
   * @param clazz
   * @param record
   * @return
   */
  public static <T extends ZNRecordDecorator> 
    T convertToTypedInstance(Class<T> clazz, ZNRecord record)
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

  public static <T extends ZNRecordDecorator> 
    List<T> convertToTypedList(Class<T> clazz, Collection<ZNRecord> records)
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

  public static <T extends ZNRecordDecorator> 
    Map<String, T> convertListToMap(List<T> records)
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
}

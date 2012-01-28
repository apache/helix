package com.linkedin.helix.store;

import java.util.Comparator;

import org.apache.log4j.Logger;

public class PropertyJsonComparator<T> implements Comparator<T>
{
  static private Logger LOG = Logger.getLogger(PropertyJsonComparator.class);
  private final PropertyJsonSerializer<T> _serializer;
  
  public PropertyJsonComparator(Class<T> clazz)
  {
    _serializer = new PropertyJsonSerializer<T>(clazz);
  }

  @Override
  public int compare(T arg0, T arg1)
  {
    if (arg0 == null && arg1 == null)
    {
      return 0;
    }
    else if (arg0 == null && arg1 != null)
    {
      return -1;
    }
    else if (arg0 != null && arg1 == null)
    {
      return 1;
    }
    else
    {
      try
      {
        String s0 = new String(_serializer.serialize(arg0));
        String s1 = new String(_serializer.serialize(arg1));

        return s0.compareTo(s1);
      }
      catch (PropertyStoreException e)
      {
        // e.printStackTrace();
        LOG.warn(e.getMessage());
        return -1;
      }
    }
  }

}

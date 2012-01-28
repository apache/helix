package com.linkedin.helix.store.zk;

import org.apache.zookeeper.data.Stat;

public class PropertyInfo<T>
{
  public T _value;
  public Stat _stat;
  public int _version;
  
  public PropertyInfo(T value, Stat stat, int version)
  {
    _value = value;
    _stat = stat;
    _version = version;
  }

}

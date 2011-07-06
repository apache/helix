package com.linkedin.clustermanager.store.zk;

import org.apache.zookeeper.data.Stat;

public class PropertyInfo
{
  public Object _value;
  public Stat _stat;
  public int _version;
  
  public PropertyInfo(Object value, Stat stat, int version)
  {
    _value = value;
    _stat = stat;
    _version = version;
  }

}

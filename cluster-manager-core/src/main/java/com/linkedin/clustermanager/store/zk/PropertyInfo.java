package com.linkedin.clustermanager.store.zk;

import java.util.List;

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
  
  public static PropertyInfo updatePropertyInfoInList(List<PropertyInfo> list, PropertyInfo propertyInfo)
  {
    if (list == null)
      return null;
    
    for(PropertyInfo info : list)
    {
      if (info._version == propertyInfo._version)
      {
        list.remove(info);
        list.add(propertyInfo);
        return info;
      }
    }
    list.add(propertyInfo);
    
    return propertyInfo;
  }
  
  public static PropertyInfo findPropertyInfoInList(List<PropertyInfo> list, int version)
  {
    if (list == null)
      return null;
    
    for(PropertyInfo info : list)
    {
      if (info._version == version)
        return info;
    }
    
    return null;
  }
  
  public static PropertyInfo removePropertyInfoFromList(List<PropertyInfo> list, int version)
  {
    if (list == null)
      return null;
    
    for(PropertyInfo info : list)
    {
      if (info._version == version)
      {
        list.remove(info);
        return info;
      }
    }
    
    return null;
  }
}

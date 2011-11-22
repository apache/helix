package com.linkedin.clustermanager.agent.file;

import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordAndStat;
import com.linkedin.clustermanager.store.PropertyStore;

public class DummyFileDataAccessor implements ClusterDataAccessor
{
  @Override
  public boolean setProperty(PropertyType type, ZNRecord value, String... keys)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean updateProperty(PropertyType type, ZNRecord value,
      String... keys)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ZNRecord getProperty(PropertyType type, String... keys)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean removeProperty(PropertyType type, String... keys)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> getChildNames(PropertyType type, String... keys)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ZNRecord> getChildValues(PropertyType type, String... keys)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PropertyStore<ZNRecord> getStore()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends ZNRecordAndStat> void refreshChildValues(Map<String, T> childValues,
         Class<T> clazz, PropertyType type, String... keys)
  {
    // TODO Auto-generated method stub
  }

}

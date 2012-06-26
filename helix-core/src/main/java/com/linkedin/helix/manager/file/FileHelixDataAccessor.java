package com.linkedin.helix.manager.file;

import java.util.List;
import java.util.Map;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyKey.Builder;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.store.file.FilePropertyStore;

public class FileHelixDataAccessor implements HelixDataAccessor
{

  public FileHelixDataAccessor(FilePropertyStore<ZNRecord> fileStore,
      String clusterName)
  {
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean createProperty(PropertyKey key, HelixProperty value)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public <T extends HelixProperty> boolean setProperty(PropertyKey key, T value)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public <T extends HelixProperty> boolean updateProperty(PropertyKey key,
      T value)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean removeProperty(PropertyKey key)
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> getChildNames(PropertyKey key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(
      PropertyKey key)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Builder keyBuilder()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends HelixProperty> boolean[] createChildren(
      List<PropertyKey> keys, List<T> children)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T extends HelixProperty> boolean[] setChildren(
      List<PropertyKey> keys, List<T> children)
  {
    return null;
  }

  @Override
  public BaseDataAccessor getBaseDataAccessor()
  {
    // TODO Auto-generated method stub
    return null;
  }

}

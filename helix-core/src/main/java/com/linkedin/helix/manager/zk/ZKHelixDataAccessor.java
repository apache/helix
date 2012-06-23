package com.linkedin.helix.manager.zk;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.BaseDataAccessor;
import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.PropertyKey;
import com.linkedin.helix.PropertyPathConfig;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;

public class ZKHelixDataAccessor implements HelixDataAccessor
{
  private static Logger LOG = Logger.getLogger(ZKHelixDataAccessor.class);
  private final BaseDataAccessor _baseDataAccessor;
  private final String _clusterName;

  public ZKHelixDataAccessor(String clusterName,
      BaseDataAccessor baseDataAccessor)
  {
    _clusterName = clusterName;
    _baseDataAccessor = baseDataAccessor;
  }

  @Override
  public boolean createProperty(PropertyKey key, HelixProperty value)
  {
    PropertyType type = key.getType();
    String path = PropertyPathConfig.getPath(type, _clusterName,
        key.getParams());
    int options = constructOptions(type);
    return _baseDataAccessor.create(path, value.getRecord(), options);
  }

  @Override
  public boolean setProperty(PropertyKey key, HelixProperty value)
  {
    PropertyType type = key.getType();
    String path = PropertyPathConfig.getPath(type, _clusterName,
        key.getParams());
    int options = constructOptions(type);
    return _baseDataAccessor.set(path, value.getRecord(), options);
  }

  @Override
  public boolean updateProperty(PropertyKey key, HelixProperty value)
  {
    PropertyType type = key.getType();
    String path = PropertyPathConfig.getPath(type, _clusterName,
        key.getParams());
    int options = constructOptions(type);
    return _baseDataAccessor.update(path, value.getRecord(), options);
  }

  @Override
  public HelixProperty getProperty(PropertyKey key)
  {
    PropertyType type = key.getType();
    String path = PropertyPathConfig.getPath(type, _clusterName,
        key.getParams());
    int options = constructOptions(type);
    ZNRecord record = _baseDataAccessor.get(path, options);
    return createPropertyObject(key.getTypeClass(), record);
  }

  @Override
  public boolean removeProperty(PropertyKey key)
  {
    PropertyType type = key.getType();
    String path = PropertyPathConfig.getPath(type, _clusterName,
        key.getParams());
    return _baseDataAccessor.remove(path);
  }

  @Override
  public List<String> getChildNames(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = PropertyPathConfig.getPath(type, _clusterName,
        key.getParams());
    int options = constructOptions(type);
    return _baseDataAccessor.getChildNames(parentPath, options);
  }

  @Override
  public List<HelixProperty> getChildValues(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = PropertyPathConfig.getPath(type, _clusterName,
        key.getParams());
    int options = constructOptions(type);
    List<ZNRecord> children = _baseDataAccessor
        .getChildren(parentPath, options);
    List<HelixProperty> childValues = new ArrayList<HelixProperty>();
    for(ZNRecord record: children){
      childValues.add(createPropertyObject(key.getTypeClass(), record));
    }
    return childValues;
  }

  @Override
  public Map<String, HelixProperty> getChildValuesMap(PropertyKey key)
  {
    PropertyType type = key.getType();
    String parentPath = PropertyPathConfig.getPath(type, _clusterName,
        key.getParams());
    int options = constructOptions(type);
    List<ZNRecord> children = _baseDataAccessor
        .getChildren(parentPath, options);
    Map<String,HelixProperty> childValuesMap = new HashMap<String,HelixProperty>();
    for(ZNRecord record: children){
      childValuesMap.put(record.getId(),createPropertyObject(key.getTypeClass(), record));
    }
    return childValuesMap;
  }

  private int constructOptions(PropertyType type)
  {
    int options = constructOptions(type);
    if (type.isPersistent())
    {
      options = options | BaseDataAccessor.Option.PERSISTENT;
    } else
    {

    }
    return options;
  }

  private HelixProperty createPropertyObject(
      Class<? extends HelixProperty> clazz, ZNRecord record)
  {
    try
    {
      Constructor<? extends HelixProperty> constructor = clazz
          .getConstructor(ZNRecord.class);
      HelixProperty property = constructor.newInstance(record);
      return property;
    } catch (Exception e)
    {
      LOG.error("Exception creating helix property instance:" + e.getMessage(),
          e);
    }
    return null;
  }
}

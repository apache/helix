package org.apache.helix.rest.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;


/**
 * A read-only wrapper of {@link ZKHelixDataAccessor} with transient cache
 * The caches is of the value from get methods and short lived for the lifecycle of one rest request
 * TODO: add more cached read method based on needs
 */
public class ZKReadAccessorWrapper extends ZKHelixDataAccessor {
  private final Map<PropertyKey, HelixProperty> _propertyCache = new HashMap<>();
  private final Map<PropertyKey, List<String>> _batchNameCache = new HashMap<>();

  public ZKReadAccessorWrapper(String clusterName, InstanceType instanceType,
      BaseDataAccessor<ZNRecord> baseDataAccessor) {
    super(clusterName, instanceType, baseDataAccessor);
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    if (_propertyCache.containsKey(key)) {
      return (T) _propertyCache.get(key);
    }
    T property = super.getProperty(key);
    _propertyCache.put(key, property);
    return property;
  }

  @Override
  public List<String> getChildNames(PropertyKey key) {
    if (_batchNameCache.containsKey(key)) {
      return _batchNameCache.get(key);
    }

    List<String> names = super.getChildNames(key);
    _batchNameCache.put(key, names);

    return names;
  }
}

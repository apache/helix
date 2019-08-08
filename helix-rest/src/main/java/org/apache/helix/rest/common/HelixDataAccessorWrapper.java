package org.apache.helix.rest.common;

import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a wrapper for {@link ZKHelixDataAccessor} that caches the result of the batch reads it
 * performs.
 * Note that the usage of this object is valid for one REST request.
 */
public class HelixDataAccessorWrapper extends ZKHelixDataAccessor {
  private final Map<PropertyKey, HelixProperty> _propertyCache = new HashMap<>();
  private final Map<PropertyKey, List<String>> _batchNameCache = new HashMap<>();

  public HelixDataAccessorWrapper(ZKHelixDataAccessor dataAccessor) {
    super(dataAccessor);
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

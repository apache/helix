package org.apache.helix.common.caches;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.PropertyKey;

public abstract class AbstractDataSnapshot<T> {
  protected final Map<PropertyKey, T> _properties;

  protected AbstractDataSnapshot(Map<PropertyKey, T> cacheData) {
    _properties = Collections.unmodifiableMap(new HashMap<>(cacheData));
  }

  public Map<PropertyKey, T> getPropertyMap() {
    return _properties;
  }
}

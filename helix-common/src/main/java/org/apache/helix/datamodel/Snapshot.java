package org.apache.helix.datamodel;

import java.util.HashMap;
import java.util.Map;


public abstract class Snapshot<K, V> {
  protected Map<K, V> _valueCache;

  public Snapshot() {
    _valueCache = new HashMap<>();
  }

  public V getValue(K key) {
    return _valueCache.get(key);
  }

  public void updateValue(K key, V value) {
    _valueCache.put(key, value);
  }

  public boolean containsKey(K key) {
    return _valueCache.containsKey(key);
  }
}

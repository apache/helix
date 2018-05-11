package org.apache.helix.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;

public class MockZkHelixDataAccessor extends ZKHelixDataAccessor {
  Map<PropertyType, Integer> _readPathCounters = new HashMap<>();

  public MockZkHelixDataAccessor(String clusterName, BaseDataAccessor<ZNRecord> baseDataAccessor) {
    super(clusterName, null, baseDataAccessor);
  }


  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) {
    for (PropertyKey key : keys) {
      addCount(key);
    }
    return super.getProperty(keys);
  }

  @Override
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys, boolean throwException) {
    for (PropertyKey key : keys) {
      addCount(key);
    }
    return super.getProperty(keys, throwException);
  }

  @Override
  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    addCount(key);
    return super.getProperty(key);
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key) {
    Map<String, T> map = super.getChildValuesMap(key);
    addCount(key, map.keySet().size());
    return map;
  }

  @Override
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key, boolean throwException) {
    Map<String, T> map = super.getChildValuesMap(key, throwException);
    addCount(key, map.keySet().size());
    return map;
  }

  private void addCount(PropertyKey key) {
    addCount(key, 1);
  }

  private void addCount(PropertyKey key, int count) {
    PropertyType type = key.getType();
    if (!_readPathCounters.containsKey(type)) {
      _readPathCounters.put(type, 0);
    }
    _readPathCounters.put(type, _readPathCounters.get(type) + count);
  }

  public int getReadCount(PropertyType type) {
    if (_readPathCounters.containsKey(type)) {
      return _readPathCounters.get(type);
    }

    return 0;
  }

  public void clearReadCounters() {
    _readPathCounters.clear();
  }
}

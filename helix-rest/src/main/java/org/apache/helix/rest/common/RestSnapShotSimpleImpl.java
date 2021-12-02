package org.apache.helix.rest.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.PropertyKey;
import org.apache.helix.rest.common.datamodel.RestSnapShot;


public class RestSnapShotSimpleImpl extends RestSnapShot {
  private final Map<PropertyKey, List<String>> _childNodesCache;

  public RestSnapShotSimpleImpl(String clusterName) {
    super(clusterName);
    _childNodesCache = new HashMap<>();
  }

  public List<String> getChildNames(PropertyKey key) {
    if (_childNodesCache.containsKey(key)) {
      return _childNodesCache.get(key);
    }
    return null;
  }

  public void updateChildNames(PropertyKey key, List<String> children) {
    _childNodesCache.put(key, children);
  }
}

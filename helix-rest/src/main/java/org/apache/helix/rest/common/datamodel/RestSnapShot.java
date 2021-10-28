package org.apache.helix.rest.common.datamodel;

import java.util.List;

import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;


public class RestSnapShot {
  private HelixDataAccessorWrapper _helixDataAccessorWrapper;

  public RestSnapShot(HelixDataAccessorWrapper wrapper) {
    _helixDataAccessorWrapper = wrapper;
  }

  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    return _helixDataAccessorWrapper.getProperty(key);
  }
  public List<String> getChildNames(PropertyKey key) {
    return _helixDataAccessorWrapper.getChildNames(key);
  }

  public PropertyKey.Builder keyBuilder() {
    return _helixDataAccessorWrapper.keyBuilder();
  }
}
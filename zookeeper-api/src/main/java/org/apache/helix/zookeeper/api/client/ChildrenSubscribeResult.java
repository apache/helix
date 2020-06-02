package org.apache.helix.zookeeper.api.client;

import java.util.Collections;
import java.util.List;


public class ChildrenSubscribeResult {
  private List<String> _children;
  private boolean _isInstalled;

  public ChildrenSubscribeResult(List<String> children, boolean isInstalled) {
    _children = Collections.unmodifiableList(children);
    _isInstalled = isInstalled;
  }

  public List<String> getChildren() {
    return _children;
  }

  public boolean isInstalled() {
    return _isInstalled;
  }
}

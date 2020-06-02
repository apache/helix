package org.apache.helix.zookeeper.api.client;

import java.util.Collections;
import java.util.List;


public class ChildrenSubscribeResult {
  private List<String> _children;
  private boolean _isInstalled;

  public ChildrenSubscribeResult(List<String> children, boolean isInstalled) {
    if (children != null) {
      _children = Collections.unmodifiableList(children);
    } else {
      _children = null;
    }
    _isInstalled = isInstalled;
  }

  public List<String> getChildren() {
    return _children;
  }

  public boolean isInstalled() {
    return _isInstalled;
  }
}

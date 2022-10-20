package org.apache.helix.metaclient.api;

import java.util.List;


public class DirectEntrySubscribeResult {
  private final List<String> _children;
  private final boolean _isInstalled;

  public DirectEntrySubscribeResult(List<String> children, boolean isInstalled) {
    _children = children;
    _isInstalled = isInstalled;
  }
}

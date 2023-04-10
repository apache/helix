package org.apache.helix.zookeeper.zkclient;

import org.apache.zookeeper.Watcher;


public interface RecursivePersistWatcherListener {
  public void handleZNodeChange(String dataPath, Watcher.Event.EventType eventType)
      throws Exception;
}

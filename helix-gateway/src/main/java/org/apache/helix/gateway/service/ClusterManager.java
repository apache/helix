package org.apache.helix.gateway.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



public class ClusterManager {
  private Map<String, Map<String, AtomicBoolean>> _flagMap;
  private Lock _lock = new ReentrantLock();

  // event queue
  // state tracker, call tracker.update

  public ClusterManager() {
    _flagMap = new ConcurrentHashMap<>();
  }

  public void addChannel() {
  }

  public void removeChannel(String instanceName) {
    _flagMap.remove(instanceName);
  }

  public AtomicBoolean sendMessage() {
    AtomicBoolean flag = new AtomicBoolean(false);
    return flag;
  }
}

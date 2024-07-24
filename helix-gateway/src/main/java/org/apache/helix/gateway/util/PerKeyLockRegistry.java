package org.apache.helix.gateway.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A registry that manages locks per key.
 */
public class PerKeyLockRegistry {
  private final ConcurrentHashMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<>();

  public void lock(String key) {
    ReentrantLock lock = lockMap.computeIfAbsent(key, k -> new ReentrantLock());
    lock.lock();
  }

  public void unlock(String key) {
    ReentrantLock lock = lockMap.get(key);
    if (lock != null) {
      lock.unlock();
    }
  }

  /**
   * Execute the action with the lock on the key
   * @param key
   * @param action
   */
  public void withLock(String key, Runnable action) {
    lock(key);
    try {
      action.run();
    } finally {
      unlock(key);
    }
  }

  /**
   * Remove the lock if it is not being used.
   * it must be called after the lock is required
   * @param key
   */
  public boolean removeLock(String key) {
    ReentrantLock lock = lockMap.get(key);
    if (lock != null && lock.isHeldByCurrentThread() && !lock.hasQueuedThreads()) {
      lockMap.remove(key, lock);
      return true;
    }
    return false;
  }
}

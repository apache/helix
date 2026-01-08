package org.apache.helix.gateway.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

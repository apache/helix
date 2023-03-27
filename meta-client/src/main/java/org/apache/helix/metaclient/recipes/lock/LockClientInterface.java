package org.apache.helix.metaclient.recipes.lock;

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

import org.apache.helix.metaclient.api.MetaClientInterface;

import java.lang.management.LockInfo;

public interface LockClientInterface {
  /**
   * Acquires a lock by creating a node at entry key and lockinfo info.
   * Will fail and return False if path and lockinfo is invalid.
   * @param key key to identify the entry
   * @param info Metadata of the lock
   * @param mode EntryMode identifying if the entry will be deleted upon client disconnect
   * @return True if the lock is acquired. False if failed to acquire (catches exception).
   */
  boolean acquireLock(String key, LockInfo info, MetaClientInterface.EntryMode mode);

  /**
   * Renews lock for a TTL Node.
   * Will fail if key is an invalid path or isn't of type TTL.
   * @param key key to identify the entry
   * @return True if the lock was renews. False if failed to renew (catches exception).
   */
  boolean renewTTLLock(String key);

  /**
   * Releases the lock by deleted the node at entry key.
   * Will fail if key is an invalid path.
   * @param key key to identify the entry
   * @return True if the lock was released. False if failed to release (catches exception).
   */
  boolean releaseLock(String key);

  /**\
   * Obtains the metadata of a lock (the LockInfo).
   * @param key key to identify the entry
   * @return LockInfo object of the node at key. If key doesn't exist, raise exception.
   */
  LockInfo retrieveLock(String key);
}

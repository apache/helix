package org.apache.helix.lock;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Generic (distributed) lock for Helix-related persisted updates
 */
public interface HelixLock {
  /**
   * Synchronously acquire a lock
   * @return true if the lock was acquired, false if could not be acquired
   */
  public boolean lock();

  /**
   * Release a lock
   * @return true if the lock was released, false if it could not be released
   */
  public boolean unlock();

  /**
   * Check if this object is blocked waiting on the lock
   * @return true if blocked, false otherwise
   */
  public boolean isBlocked();
}

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
 * Generic interface for Helix distributed lock
 */
public interface DistributedLock {
  /**
   * Blocking call to acquire a lock
   * @return true if the lock was successfully acquired,
   * false if the lock could not be acquired
   */
  boolean acquireLock();

  /**
   * Blocking call to release a lock
   * @return true if the lock was successfully released or if the locked is not currently locked,
   * false if the lock is not locked by the user or the release operation failed
   */
  boolean releaseLock();

  /**
   * Retrieve the information of the current lock on the resource this lock object specifies, e.g. lock timeout, lock message, etc.
   * @return lock metadata information
   */
  LockInfo getCurrentLockInfo();

  /**
   * If the user is current lock owner of the resource
   * @return true if the user is the lock owner,
   * false if the user is not the lock owner or the lock doesn't have a owner
   */
  boolean isCurrentOwner();
}

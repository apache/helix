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

package org.apache.helix.lock.helix;

import java.util.Date;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.lock.LockInfo;
import org.apache.helix.lock.LockScope;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.log4j.Logger;


/**
 * Helix nonblocking lock implementation based on Zookeeper
 */
public class ZKHelixNonblockingLock implements HelixLock {

  private static final Logger LOG = Logger.getLogger(ZKHelixNonblockingLock.class);

  private final String _lockPath;
  private final String _userId;
  private final long _timeout;
  private final String _lockMsg;
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;

  /**
   * Initialize the lock with user provided information, e.g.,cluster, scope, etc.
   * @param scope the scope to lock
   * @param zkAddress the zk address the cluster connects to
   * @param timeout the timeout period of the lcok
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   */
  public ZKHelixNonblockingLock(LockScope scope, String zkAddress, Long timeout, String lockMsg,
      String userId) {
    this(scope.getPath(), zkAddress, timeout, lockMsg, userId);
  }

  /**
   * Initialize the lock with user provided information, e.g., lock path under zookeeper, etc.
   * @param lockPath the path of the lock under Zookeeper
   * @param zkAddress the zk address of the cluster
   * @param timeout the timeout period of the lcok
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   */
  private ZKHelixNonblockingLock(String lockPath, String zkAddress, Long timeout, String lockMsg,
      String userId) {
    _lockPath = lockPath;
    _timeout = timeout;
    _lockMsg = lockMsg;
    _userId = userId;
    _baseDataAccessor = new ZkBaseDataAccessor<>(zkAddress);
  }

  @Override
  public boolean acquireLock() {

    // Set lock information fields
    long deadline;
    // Prevent value overflow
    if (_timeout > Long.MAX_VALUE - System.currentTimeMillis()) {
      deadline = Long.MAX_VALUE;
    } else {
      deadline = System.currentTimeMillis() + _timeout;
    }
    LockInfo lockInfo = new LockInfo(_userId);
    lockInfo.setLockInfoFields(_userId, _lockMsg, deadline);

    LockUpdater updater = new LockUpdater(lockInfo);
    return _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);
  }

  @Override
  public boolean releaseLock() {
    // Initialize the lock updater with a default lock info represents the state of a unlocked lock
    LockUpdater updater = new LockUpdater(LockInfo.defaultLockInfo);
    return _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);
  }

  @Override
  public LockInfo getLockInfo() {
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    return new LockInfo(curLockInfo);
  }

  @Override
  public boolean isOwner() {
    LockInfo lockInfo = getLockInfo();
    return userIdMatches(lockInfo) && !hasTimedOut(lockInfo);
  }

  /**
   * Check if a lock has timed out
   * @return return true if the lock has timed out, otherwise return false.
   */
  private boolean hasTimedOut(LockInfo lockInfo) {
    return System.currentTimeMillis() >= lockInfo.getTimeout();
  }

  /**
   * Check if a lock has timed out with lock information stored in a ZNRecord
   * @return return true if the lock has timed out, otherwise return false.
   */
  private boolean hasTimedOut(ZNRecord znRecord) {
    return System.currentTimeMillis() >= LockInfo.getTimeout(znRecord);
  }

  /**
   * Check if the current user Id matches with the owner Id in a lock info
   * @return return true if the two ids match, otherwise return false.
   */
  private boolean userIdMatches(LockInfo lockInfo) {
    return lockInfo.getOwner().equals(_userId);
  }

  /**
   * Check if a user id in the ZNRecord matches current user's id
   * @param znRecord ZNRecord contains lock information
   * @return true if the ids match, false if not or ZNRecord does not contain user id information
   */
  private boolean userIdMatches(ZNRecord znRecord) {
    return LockInfo.getOwner(znRecord).equals(_userId);
  }

  /**
   * Check if the lock node has a current owner
   * @param znRecord Lock information in format of ZNRecord
   * @return true if the lock has a current owner that the ownership has not be timed out, otherwise false
   */
  private boolean hasNonExpiredOwner(ZNRecord znRecord) {
    return LockInfo.ownerIdSet(znRecord) && !hasTimedOut(znRecord);
  }

  /**
   * Class that specifies how a lock node should be updated with another lock node
   */
  private class LockUpdater implements DataUpdater<ZNRecord> {
    final ZNRecord _record;

    /**
     * Initialize a structure for lock user to update a lock node value
     * @param lockInfo the lock node value will be used to update the lock
     */
    public LockUpdater(LockInfo lockInfo) {
      _record = lockInfo.getRecord();
    }

    @Override
    public ZNRecord update(ZNRecord current) {
      // If no one owns the lock, allow the update
      // If the lock owner id matches user id, allow the update
      if (!hasNonExpiredOwner(current) || userIdMatches(current)) {
        return _record;
      }
      // For users who are not the lock owner and try to do an update on a lock that is held by someone else, exception thrown is to be caught by data accessor, and return false for the update
      LOG.error(
          "User " + _userId + "tried to update the lock at " + new Date(System.currentTimeMillis())
              + ". Lock path: " + _lockPath);
      throw new HelixException("User is not authorized to perform this operation.");
    }
  }
}

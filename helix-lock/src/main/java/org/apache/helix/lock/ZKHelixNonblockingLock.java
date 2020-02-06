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

package org.apache.helix.lock;

import java.util.Date;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import static org.apache.helix.lock.ZKHelixNonblockingLockInfo.DEFAULT_OWNER_TEXT;
import static org.apache.helix.lock.ZKHelixNonblockingLockInfo.DEFAULT_TIMEOUT_LONG;


/**
 * Helix nonblocking lock implementation based on Zookeeper
 */
public class ZKHelixNonblockingLock implements HelixLock {

  private static final Logger LOG = Logger.getLogger(ZKHelixNonblockingLock.class);

  private static final String LOCK_ROOT = "LOCKS";
  private static final String PATH_DELIMITER = "/";
  private final String _lockPath;
  private final String _userId;
  private final long _timeout;
  private final String _lockMsg;
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;

  /**
   * Initialize the lock with user provided information, e.g.,cluster, scope, etc.
   * @param clusterName the cluster under which the lock will live
   * @param scope the scope to lock
   * @param zkAddress the zk address the cluster connects to
   * @param timeout the timeout period of the lcok
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   */
  public ZKHelixNonblockingLock(String clusterName, HelixConfigScope scope, String zkAddress,
      Long timeout, String lockMsg, String userId) {
    this(PATH_DELIMITER +  String.join(PATH_DELIMITER, clusterName, LOCK_ROOT, scope.getZkPath()), zkAddress, timeout, lockMsg,
        userId);
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
    ZNRecord lockInfo = new ZNRecord(_userId);
    lockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name(), _userId);
    lockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.MESSAGE.name(), _lockMsg);

    long deadline;
    // Prevent value overflow
    if (_timeout > Long.MAX_VALUE - System.currentTimeMillis()) {
      deadline = Long.MAX_VALUE;
    } else {
      deadline = System.currentTimeMillis() + _timeout;
    }
    lockInfo.setLongField(ZKHelixNonblockingLockInfo.InfoKey.TIMEOUT.name(), deadline);

    // Try to create the lock node
    boolean success = _baseDataAccessor.create(_lockPath, lockInfo, AccessOption.PERSISTENT);

    // If fail to create the lock node (acquire the lock), compare the timeout timestamp of current lock node with current time, if already passes the timeout, release current lock and try to acquire the lock again
    if (!success) {
      Stat stat = new Stat();
      ZNRecord curLock = _baseDataAccessor.get(_lockPath, stat, AccessOption.PERSISTENT);
      if (hasTimedOut(curLock)) {
        // set may fail when the znode version changes in between the get and set, meaning there are some changes in the lock
        success =
            _baseDataAccessor.set(_lockPath, lockInfo, stat.getVersion(), AccessOption.PERSISTENT);
      }
    }
    return success;
  }

  @Override
  public boolean releaseLock() {
    ZNRecord newLockInfo = new ZKHelixNonblockingLockInfo<>().toZNRecord();
    LockUpdater updater = new LockUpdater(newLockInfo);
    return _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);
  }

  @Override
  public ZKHelixNonblockingLockInfo getLockInfo() {
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    return new ZKHelixNonblockingLockInfo(curLockInfo);
  }

  @Override
  public boolean isOwner() {
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    return userIdMatches(curLockInfo) && !hasTimedOut(curLockInfo);
  }

  /**
   * Check if a lock has timed out
   * @param record the current lock information in ZNRecord format
   * @return return true if the lock has timed out, otherwise return false.
   */
  private boolean hasTimedOut(ZNRecord record) {
    if (record == null) {
      return false;
    }
    long timeout = record
        .getLongField(ZKHelixNonblockingLockInfo.InfoKey.TIMEOUT.name(), DEFAULT_TIMEOUT_LONG);
    return System.currentTimeMillis() >= timeout;
  }

  /**
   * Check if the current user Id matches with the owner Id in a lock info
   * @param record the lock information in ZNRecord format
   * @return return true if the two ids match, otherwise return false.
   */
  private boolean userIdMatches(ZNRecord record) {
    if (record == null) {
      return false;
    }
    String ownerId = record.getSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name());
    return ownerId.equals(_userId);
  }

  /**
   * Check if the lock node has a owner id field in the lock information
   * @param record Lock information in format of ZNRecord
   * @return true if the lock has a owner id field that the ownership can be or not be timed out, otherwise false
   */
  private boolean hasOwnerField(ZNRecord record) {
    if (record == null) {
      return false;
    }
    String ownerId = record.getSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name());
    return ownerId != null && !ownerId.equals(DEFAULT_OWNER_TEXT);
  }

  /**
   * Check if the lock node has a current owner
   * @param record Lock information in format of ZNRecord
   * @return true if the lock has a current owner that the ownership has not be timed out, otherwise false
   */
  private boolean hasNonExpiredOwner(ZNRecord record) {
    return hasOwnerField(record) && !hasTimedOut(record);
  }

  /**
   * Class that specifies how a lock node should be updated with another lock node
   */
  private class LockUpdater implements DataUpdater<ZNRecord> {
    final ZNRecord _record;

    /**
     * Initialize a structure for lock user to update a lock node value
     * @param record the lock node value will be updated in ZNRecord format
     */
    public LockUpdater(ZNRecord record) {
      _record = record;
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

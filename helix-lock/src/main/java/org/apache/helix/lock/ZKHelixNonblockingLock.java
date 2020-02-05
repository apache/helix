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

import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;


/**
 * Helix nonblocking lock implementation based on Zookeeper
 */
public class ZKHelixNonblockingLock implements HelixLock {

  private static final Logger LOG = Logger.getLogger(ZKHelixNonblockingLock.class);

  private static final String LOCK_ROOT = "LOCKS";
  private static final String PATH_DELIMITER = "/";
  private static final String UUID_FORMAT_REGEX =
      "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
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
    this(PATH_DELIMITER + clusterName + PATH_DELIMITER + LOCK_ROOT + PATH_DELIMITER + scope,
        zkAddress, timeout, lockMsg, userId);
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
    HelixZkClient zkClient = new ZkClient(zkAddress);
    _lockPath = lockPath;
    _timeout = timeout;
    _lockMsg = lockMsg;
    if (!userId.matches(UUID_FORMAT_REGEX)) {
      throw new IllegalArgumentException("The input user id is not a valid UUID.");
    }
    _userId = userId;
    _baseDataAccessor = new ZkBaseDataAccessor<ZNRecord>(zkClient.getServers());
  }

  @Override
  public boolean acquireLock() {

    // Set lock information fields
    ZNRecord lockInfo = new ZNRecord(_userId);
    lockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name(), _userId);
    lockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.MESSAGE.name(), _lockMsg);
    long timeout;

    // If the input timeout value is the max value, set the expire time to max value
    if (_timeout == Long.MAX_VALUE) {
      timeout = _timeout;
    } else {
      timeout = System.currentTimeMillis() + _timeout;
    }
    lockInfo
        .setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.TIMEOUT.name(), String.valueOf(timeout));

    // Try to create the lock node
    boolean success = _baseDataAccessor.create(_lockPath, lockInfo, AccessOption.PERSISTENT);

    // If fail to create the lock node (acquire the lock), compare the timeout timestamp of current lock node with current time, if already passes the timeout, release current lock and try to acquire the lock again
    if (!success) {
      Stat stat = new Stat();
      ZNRecord curLock = _baseDataAccessor.get(_lockPath, stat, AccessOption.PERSISTENT);
      long curTimeout =
          Long.parseLong(curLock.getSimpleField(ZKHelixNonblockingLockInfo.InfoKey.TIMEOUT.name()));
      if (System.currentTimeMillis() >= curTimeout) {
        // set may fail when the znode version changes in between the get and set, meaning there are some changes in the lock
        success =
            _baseDataAccessor.set(_lockPath, lockInfo, stat.getVersion(), AccessOption.PERSISTENT);
      }
    }
    return success;
  }

  @Override
  public boolean releaseLock() {
    ZNRecord newLockInfo = new ZNRecord(_userId);
    newLockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name(),
        ZKHelixNonblockingLockInfo.DEFAULT_OWNER_TEXT);
    newLockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.MESSAGE.name(),
        ZKHelixNonblockingLockInfo.DEFAULT_MESSAGE_TEXT);
    newLockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.TIMEOUT.name(),
        ZKHelixNonblockingLockInfo.DEFAULT_TIMEOUT_TEXT);
    LockUpdater updater = new LockUpdater(newLockInfo);
    return _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);
  }

  @Override
  public ZKHelixNonblockingLockInfo getLockInfo() {
    if (!_baseDataAccessor.exists(_lockPath, AccessOption.PERSISTENT)) {
      return new ZKHelixNonblockingLockInfo();
    }
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    return new ZKHelixNonblockingLockInfo(curLockInfo);
  }

  @Override
  public boolean isOwner() {
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    if (curLockInfo == null) {
      return false;
    }
    return userIdMatches(curLockInfo) && !hasTimedOut(curLockInfo);
  }

  /**
   * Check if a lock has timed out
   * @param record the current lock information in ZNRecord format
   * @return return true if the lock has timed out, otherwise return false.
   */
  private boolean hasTimedOut(ZNRecord record) {
    String timeoutStr = record.getSimpleField(ZKHelixNonblockingLockInfo.InfoKey.TIMEOUT.name());
    return System.currentTimeMillis() >= Long.parseLong(timeoutStr);
  }

  /**
   * Check if the current user Id matches with the owner Id in a lock info
   * @param record the lock information in ZNRecord format
   * @return return true if the two ids match, otherwise return false.
   */
  private boolean userIdMatches(ZNRecord record) {
    String ownerId = record.getSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name());
    return ownerId.equals(_userId);
  }

  /**
   * Class that specifies how a lock node should be updated with another lock node for a lock owner only
   */
  private class LockUpdater implements DataUpdater<ZNRecord> {
    final ZNRecord _record;

    /**
     * Initialize a structure for lock owner to update a lock node value
     * @param record the lock node value will be updated in ZNRecord format
     */
    public LockUpdater(ZNRecord record) {
      _record = record;
    }

    @Override
    public ZNRecord update(ZNRecord current) {
      if (current != null && userIdMatches(current) && !hasTimedOut(current)) {
        return _record;
      }
      throw new HelixException("User is not authorized to perform this operation.");
    }
  }
}

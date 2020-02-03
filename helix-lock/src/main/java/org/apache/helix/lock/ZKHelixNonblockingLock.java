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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZNRecordUpdater;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.util.ZNRecordUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;


/**
 * Helix nonblocking lock implementation based on Zookeeper
 */
public class ZKHelixNonblockingLock implements HelixLock {

  private static final Logger LOG = Logger.getLogger(ZKHelixNonblockingLock.class);

  private static final String LOCK_ROOT = "LOCKS";
  private final String _lockPath;
  private final String _userID;
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
   */
  public ZKHelixNonblockingLock(String clusterName, HelixConfigScope scope, String zkAddress,
      Long timeout, String lockMsg, String userID) {
    this("/" + clusterName + '/' + LOCK_ROOT + '/' + scope, zkAddress, timeout, lockMsg, userID);
  }

  /**
   * Initialize the lock with user provided information, e.g., lock path under zookeeper, etc.
   * @param lockPath the path of the lock under Zookeeper
   * @param zkAddress the zk address of the cluster
   * @param timeout the timeout period of the lcok
   * @param lockMsg the reason for having this lock
   */
  public ZKHelixNonblockingLock(String lockPath, String zkAddress, Long timeout, String lockMsg,
      String userID) {
    HelixZkClient zkClient = new ZkClient(zkAddress);
    _lockPath = lockPath;
    _timeout = timeout;
    _lockMsg = lockMsg;
    _userID = userID;
    _baseDataAccessor = new ZkBaseDataAccessor<ZNRecord>(zkClient.getServers());
  }

  @Override
  /**
   * Blocking call to acquire a lock
   * @return true if the lock was successfully acquired,
   * false if the lock could not be acquired
   */ public boolean acquireLock() {

    // Set lock information fields
    ZNRecord lockInfo = new ZNRecord(_userID);
    lockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name(), _userID);
    lockInfo.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.MESSAGE.name(), _lockMsg);
    long timeout = System.currentTimeMillis() + _timeout;
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
        success =
            _baseDataAccessor.set(_lockPath, lockInfo, stat.getVersion(), AccessOption.PERSISTENT);
      }
    }
    return success;
  }

  @Override
  /**
   * Blocking call to release a lock
   * @return true if the lock was successfully released,
   * false if the locked is not locked or is not locked by the user,
   * or the lock could not be released
   */ public boolean releaseLock() {
    if (isOwner()) {
      return _baseDataAccessor.remove(_lockPath, AccessOption.PERSISTENT);
    }
    return false;
  }

  @Override
  /**
   * Retrieve the lock information, e.g. lock timeout, lock message, etc.
   * @return lock metadata information, return null if there is no lock node for the path provided
   */ public LockInfo<String> getLockInfo() {
    if (!_baseDataAccessor.exists(_lockPath, AccessOption.PERSISTENT)) {
      return null;
    }
    ZKHelixNonblockingLockInfo<String> lockInfo = new ZKHelixNonblockingLockInfo<>();
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    lockInfo.setLockInfoFields(curLockInfo);
    return lockInfo;
  }

  @Override
  /**
   * Check if the user is current lock owner
   * @return true if the user is the lock owner,
   * false if the user is not the lock owner or the lock doesn't have a owner
   */ public boolean isOwner() {
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    if (curLockInfo == null) {
      return false;
    }
    String ownerID = curLockInfo.getSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name());
    if (ownerID == null) {
      return false;
    }
    return ownerID.equals(_userID);
  }
}

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
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.util.ZNRecordUtil;
import org.apache.log4j.Logger;


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
  public boolean acquireLock() {

    // Set lock information fields
    ZNRecord lockInfo = new ZNRecord(_userID);
    lockInfo.setSimpleField("Owner", _userID);
    lockInfo.setSimpleField("message", _lockMsg);
    long timeout = System.currentTimeMillis() + _timeout;
    lockInfo.setSimpleField("timeout", String.valueOf(timeout));

    // Try to create the lock node
    boolean success = _baseDataAccessor.create(_lockPath, lockInfo, AccessOption.PERSISTENT);

    // If fail to create the lock node, compare the timeout timestamp of current lock node with current time, if already passes the timeout, delete current lock node and try to create lock node again
    if (!success) {
      ZNRecord curLock = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
      long curTimeout = Long.parseLong(curLock.getSimpleField("timeout"));
      if (System.currentTimeMillis() >= curTimeout) {
        _baseDataAccessor.remove(_lockPath, AccessOption.PERSISTENT);
        return _baseDataAccessor.create(_lockPath, lockInfo, AccessOption.PERSISTENT);
      }
    }
    return success;
  }

  @Override
  public boolean releaseLock() {
    if (isOwner()) {
      return _baseDataAccessor.remove(_lockPath, AccessOption.PERSISTENT);
    }
    return false;
  }

  @Override
  public LockInfo<String> getLockInfo() {
    ZKHelixNonblockingLockInfo<String> lockInfo = new ZKHelixNonblockingLockInfo<>();
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    lockInfo.setLockInfoFields(curLockInfo);
    return lockInfo;
  }

  @Override
  public boolean isOwner() {
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    if (curLockInfo == null) {
      return false;
    }
    String ownerID = curLockInfo.getSimpleField("owner");
    if (ownerID == null) {
      return false;
    }
    return ownerID.equals(_userID);
  }
}

package org.apache.helix.lock.helix;

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

import java.util.Date;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.lock.DistributedLock;
import org.apache.helix.lock.LockInfo;
import org.apache.helix.lock.LockScope;
import org.apache.helix.manager.zk.GenericZkHelixApiBuilder;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.log4j.Logger;


/**
 * Helix nonblocking lock implementation based on Zookeeper.
 * NOTE: do NOT use ephemeral nodes in this implementation because ephemeral mode is not supported
 * in ZooScalability mode.
 */
public class ZKDistributedNonblockingLock implements DistributedLock {
  private static final Logger LOG = Logger.getLogger(ZKDistributedNonblockingLock.class);

  private final String _lockPath;
  private final String _userId;
  private final long _timeout;
  private final String _lockMsg;
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;

  /**
   * Initialize the lock with user provided information, e.g.,cluster, scope, etc.
   * @param scope the scope to lock
   * @param zkAddress the zk address the cluster connects to
   * @param timeout the timeout period of the lock
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   */
  public ZKDistributedNonblockingLock(LockScope scope, String zkAddress, Long timeout,
      String lockMsg, String userId) {
    this(scope.getPath(), timeout, lockMsg, userId, new ZkBaseDataAccessor<ZNRecord>(zkAddress));
  }

  /**
   * Initialize the lock with user provided information, e.g., lock path under zookeeper, etc.
   * @param lockPath the path of the lock under Zookeeper
   * @param timeout the timeout period of the lock
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   * @param baseDataAccessor baseDataAccessor instance to do I/O against ZK with
   */
  private ZKDistributedNonblockingLock(String lockPath, Long timeout, String lockMsg, String userId,
      BaseDataAccessor<ZNRecord> baseDataAccessor) {
    _lockPath = lockPath;
    if (timeout < 0) {
      throw new IllegalArgumentException("The expiration time cannot be negative.");
    }
    _timeout = timeout;
    _lockMsg = lockMsg;
    _userId = userId;
    _baseDataAccessor = baseDataAccessor;
  }

  @Override
  public boolean tryLock() {
    // Set lock information fields
    long deadline;
    // Prevent value overflow
    if (_timeout > Long.MAX_VALUE - System.currentTimeMillis()) {
      deadline = Long.MAX_VALUE;
    } else {
      deadline = System.currentTimeMillis() + _timeout;
    }
    LockUpdater updater = new LockUpdater(new LockInfo(_userId, _lockMsg, deadline));
    return _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);
  }

  //TODO: update release lock logic so it would not leave empty znodes after the lock is released
  @Override
  public boolean unlock() {
    // Initialize the lock updater with a default lock info represents the state of a unlocked lock
    LockUpdater updater = new LockUpdater(LockInfo.defaultLockInfo);
    return _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);
  }

  @Override
  public LockInfo getCurrentLockInfo() {
    ZNRecord curLockInfo = _baseDataAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
    return new LockInfo(curLockInfo);
  }

  @Override
  public boolean isCurrentOwner() {
    LockInfo lockInfo = getCurrentLockInfo();
    return lockInfo.getOwner().equals(_userId) && (System.currentTimeMillis() < lockInfo
        .getTimeout());
  }

  @Override
  public void close() {
    if (isCurrentOwner()) {
      throw new HelixException("Please unlock the lock before closing it.");
    }
    _baseDataAccessor.close();
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
      // If the user is the current lock owner, allow the update
      LockInfo curLockInfo = new LockInfo(current);
      if (!(System.currentTimeMillis() < curLockInfo.getTimeout()) || isCurrentOwner()) {
        return _record;
      }
      // For users who are not the lock owner and try to do an update on a lock that is held by
      // someone else, exception thrown is to be caught by data accessor, and return false for
      // the update
      LOG.error(
          "User " + _userId + " tried to update the lock at " + new Date(System.currentTimeMillis())
              + ". Lock path: " + _lockPath);
      throw new HelixException("User is not authorized to perform this operation.");
    }
  }

  /**
   * Builder class to use with ZKDistributedNonblockingLock.
   */
  public static class Builder extends GenericZkHelixApiBuilder<Builder> {
    private LockScope _lockScope;
    private String _userId;
    private long _timeout;
    private String _lockMsg;

    public Builder() {
    }

    public void setLockScope(LockScope lockScope) {
      _lockScope = lockScope;
    }

    public void setUserId(String userId) {
      _userId = userId;
    }

    public void setTimeout(long timeout) {
      _timeout = timeout;
    }

    public void setLockMsg(String lockMsg) {
      _lockMsg = lockMsg;
    }

    public ZKDistributedNonblockingLock build() {
      // Resolve which way we want to create BaseDataAccessor instance
      BaseDataAccessor<ZNRecord> baseDataAccessor;
      // If enabled via System.Properties config or the given zkAddress is null, use ZooScalability
      if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED) || _zkAddress == null) {
        // If the multi ZK config is enabled, use multi-realm mode with FederatedZkClient
        baseDataAccessor = new ZkBaseDataAccessor.Builder<ZNRecord>().setRealmMode(_realmMode)
            .setRealmAwareZkClientConfig(_realmAwareZkClientConfig)
            .setRealmAwareZkConnectionConfig(_realmAwareZkConnectionConfig).setZkAddress(_zkAddress)
            .build();
      } else {
        baseDataAccessor = new ZkBaseDataAccessor<>(_zkAddress);
      }

      // Return a ZKDistributedNonblockingLock instance
      return new ZKDistributedNonblockingLock(_lockScope.getPath(), _timeout, _lockMsg, _userId,
          baseDataAccessor);
    }
  }
}

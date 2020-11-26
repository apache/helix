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
import org.apache.helix.zookeeper.datamodel.ZNRecordUpdater;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.lock.LockInfo.DEFAULT_PRIORITY_INT;
import static org.apache.helix.lock.LockInfo.DEFAULT_REQUESTOR_ID;
import static org.apache.helix.lock.LockInfo.DEFAULT_REQUESTOR_PRIORITY_INT;
import static org.apache.helix.lock.LockInfo.DEFAULT_REQUESTOR_REQUESTING_TIMESTAMP_LONG;
import static org.apache.helix.lock.LockInfo.DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG;
import static org.apache.helix.lock.LockInfo.DEFAULT_WAITING_TIMEOUT_LONG;
import static org.apache.helix.lock.LockInfo.ZNODE_ID;


/**
 * Helix nonblocking lock implementation based on Zookeeper.
 * NOTE: do NOT use ephemeral nodes in this implementation because ephemeral mode is not supported
 * in ZooScalability mode.
 */
public class ZKDistributedNonblockingLock implements DistributedLock, IZkDataListener {
  private static final Logger LOG = LoggerFactory.getLogger(ZKDistributedNonblockingLock.class);

  private final String _lockPath;
  private final String _userId;
  private final String _lockMsg;
  private final long _leaseTimeout;
  private final long _waitingTimeout;
  private final long _cleanupTimeout;
  private final int _priority;
  private final boolean _isForceful;
  private final LockListener _lockListener;
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;
  private boolean _isLocked;
  private boolean _isPending;
  private boolean _isPreempted;
  private long _pendingTimeout;

  /**
   * Initialize the lock with user provided information, e.g.,cluster, scope, etc.
   * @param scope the scope to lock
   * @param zkAddress the zk address the cluster connects to
   * @param leaseTimeout the leasing timeout period of the lock
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   */
  public ZKDistributedNonblockingLock(LockScope scope, String zkAddress, Long leaseTimeout,
      String lockMsg, String userId) {
    this(scope.getPath(), leaseTimeout, lockMsg, userId, 0, Integer.MAX_VALUE, 0, false,
        new LockListener() {
          @Override
          public void onCleanupNotification() {
          }
        }, new ZkBaseDataAccessor<ZNRecord>(zkAddress));
  }

  /**
   * Initialize the lock with user provided information, e.g.,cluster, scope, etc.
   * @param scope the scope to lock
   * @param zkAddress the zk address the cluster connects to
   * @param leaseTimeout the leasing timeout period of the lock
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   * @param priority the priority of the lock
   * @param waitingTimeout the waiting timeout period of the lock when the tryLock request is issued
   * @param cleanupTimeout the time period needed to finish the cleanup work by the lock when it
   *                      is preempted
   * @param isForceful whether the lock is a forceful one. This determines the behavior when the
   *                   lock encountered an exception during preempting lower priority lock
   * @param lockListener the listener associated to the lock
   */
  public ZKDistributedNonblockingLock(LockScope scope, String zkAddress, Long leaseTimeout,
      String lockMsg, String userId, int priority, long waitingTimeout, long cleanupTimeout,
      boolean isForceful, LockListener lockListener) {
    this(scope.getPath(), leaseTimeout, lockMsg, userId, priority, waitingTimeout, cleanupTimeout,
        isForceful, lockListener, new ZkBaseDataAccessor<ZNRecord>(zkAddress));
  }

  /**
   * Initialize the lock with user provided information, e.g., lock path under zookeeper, etc.
   * @param lockPath the path of the lock under Zookeeper
   * @param leaseTimeout the leasing timeout period of the lock
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   * @param priority the priority of the lock
   * @param waitingTimeout the waiting timeout period of the lock when the tryLock request is issued
   * @param cleanupTimeout the time period needed to finish the cleanup work by the lock when it
   *                      is preempted
   * @param isForceful whether the lock is a forceful one. This determines the behavior when the
   *                   lock encountered an exception during preempting lower priority lock
   * @param lockListener the listener associated to the lock
   * @param baseDataAccessor baseDataAccessor instance to do I/O against ZK with
   */
  private ZKDistributedNonblockingLock(String lockPath, Long leaseTimeout, String lockMsg,
      String userId, int priority, long waitingTimeout, long cleanupTimeout, boolean isForceful,
      LockListener lockListener, BaseDataAccessor<ZNRecord> baseDataAccessor) {
    _lockPath = lockPath;
    if (leaseTimeout < 0 || waitingTimeout < 0 || cleanupTimeout < 0) {
      throw new IllegalArgumentException("Timeout cannot be negative.");
    }
    if (priority < 0) {
      throw new IllegalArgumentException("Priority cannot be negative.");
    }
    _leaseTimeout = leaseTimeout;
    _lockMsg = lockMsg;
    _userId = userId;
    _baseDataAccessor = baseDataAccessor;
    _priority = priority;
    _waitingTimeout = waitingTimeout;
    _cleanupTimeout = cleanupTimeout;
    _lockListener = lockListener;
    _isForceful = isForceful;
  }

  @Override
  public synchronized boolean tryLock() {
    // Set lock information fields
    _baseDataAccessor.subscribeDataChanges(_lockPath, this);
    LockUpdater updater = new LockUpdater(
        new LockInfo(_userId, _lockMsg, getNonOverflowTimestamp(_leaseTimeout), _priority,
            _waitingTimeout, _cleanupTimeout, null, 0, 0, 0));
    boolean updateResult = _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);

    // Check whether the lock request is still pending. If yes, we will wait for the period
    // recorded in _pendingTimeout.
    if (_isPending) {
      try {
        wait(_pendingTimeout);
      } catch (InterruptedException e) {
        throw new HelixException(
            String.format("Interruption happened while %s is waiting for the " + "lock", _userId),
            e);
      }
      // We have not acquired the lock yet.
      if (!_isLocked) {
        // If the reason for not being able to acquire the lock is due to high priority lock
        // preemption, directly return false.
        if (_isPreempted) {
          return false;
        } else {
          // Forceful lock request will grab the lock even the current owner has not finished
          // cleanup work, while non forceful lock request will get an exception.
          if (_isForceful) {
            ZNRecord znRecord = composeNewOwnerRecord();
            LOG.info("Updating Zookeeper with new owner {} information", _userId);
            _baseDataAccessor
                .update(_lockPath, new ZNRecordUpdater(znRecord), AccessOption.PERSISTENT);
            return true;
          } else {
            throw new HelixException("Cleanup has not been finished by lock owner");
          }
        }
      }
    }
    return updateResult;
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

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
    Stat stat = new Stat();
    ZNRecord readData =
        _baseDataAccessor.get(dataPath, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);
    LockInfo lockInfo = new LockInfo(readData);
    // We are the current owner
    if (lockInfo.getOwner().equals(_userId)) {
      if (lockInfo.getRequestorId() == null || lockInfo.getPriority() > lockInfo
          .getRequestorPriority()) {
        LOG.info("We do not need to handle this data change");
      } else {
        _lockListener.onCleanupNotification();
        // read the lock information again to avoid stale data
        readData = _baseDataAccessor.get(dataPath, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);
        // If we are still the lock owner, clean the lock owner field.
        if (lockInfo.getOwner().equals(_userId)) {
          ZNRecord znRecord = new ZNRecord(readData);
          znRecord.setSimpleField(LockInfo.LockInfoAttribute.OWNER.name(), "");
          znRecord.setSimpleField(LockInfo.LockInfoAttribute.MESSAGE.name(), "");
          znRecord.setLongField(LockInfo.LockInfoAttribute.TIMEOUT.name(), -1);
          znRecord.setIntField(LockInfo.LockInfoAttribute.PRIORITY.name(), -1);
          znRecord.setLongField(LockInfo.LockInfoAttribute.WAITING_TIMEOUT.name(), -1);
          znRecord.setLongField(LockInfo.LockInfoAttribute.CLEANUP_TIMEOUT.name(), -1);
          _baseDataAccessor
              .update(_lockPath, new ZNRecordUpdater(znRecord), AccessOption.PERSISTENT);
        } else {
          LOG.info("We are not current lock owner");
        }
      }
    } // We are the current requestor
    else if (lockInfo.getRequestorId().equals(_userId)) {
      // In case the owner field is empty, it means previous owner has finished cleanup work.
      if (lockInfo.getOwner().isEmpty()) {
        ZNRecord znRecord = composeNewOwnerRecord();
        _baseDataAccessor.update(_lockPath, new ZNRecordUpdater(znRecord), AccessOption.PERSISTENT);
        onAcquiredLockNotification();
      } else {
        LOG.info("We do not need to handle this data change");
      }
    } // If we are waiting for the lock, but find we are not the requestor any more, meaning we
    // are preempted by an even higher priority request
    else if (lockInfo.getRequestorId() != null && !lockInfo.getRequestorId().equals(_userId)
        && _isPending) {
      onDeniedPendingLockNotification();
    }
  }

  /**
   * call back called when the lock is acquired
   */
  public void onAcquiredLockNotification() {
    synchronized (ZKDistributedNonblockingLock.this) {
      _isLocked = true;
      ZKDistributedNonblockingLock.this.notify();
    }
  }

  /**
   * call back called when the pending request is denied due to another higher priority request
   */
  public void onDeniedPendingLockNotification() {
    synchronized (ZKDistributedNonblockingLock.this) {
      _isLocked = false;
      _isPreempted = true;
      ZKDistributedNonblockingLock.this.notify();
    }
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
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
      if (System.currentTimeMillis() > curLockInfo.getTimeout() || isCurrentOwner()) {
        return _record;
      }

      // higher priority lock request will preempt current lock owner that is with lower priority
      if (!isCurrentOwner() && _priority > curLockInfo.getPriority() && curLockInfo.getRequestorId()
          .isEmpty()) {
        // update lock Znode with requestor information
        ZNRecord newRecord = composeNewRequestorRecord(curLockInfo, _record);
        _isPending = true;
        _pendingTimeout =
            _waitingTimeout > curLockInfo.getCleanupTimeout() ? curLockInfo.getCleanupTimeout()
                : _waitingTimeout;
        return newRecord;
      }

      // If the requestor field is not empty, and the coming lock request has a even higher
      // priority. The new request will replace current requestor field of the lock
      if (!isCurrentOwner() && _priority > curLockInfo.getPriority() && !curLockInfo
          .getRequestorId().isEmpty() && _priority > curLockInfo.getRequestorPriority()) {
        ZNRecord newRecord = composeNewRequestorRecord(curLockInfo, _record);
        _isPending = true;
        long remainingCleanupTime =
            curLockInfo.getCleanupTimeout() - (System.currentTimeMillis() - curLockInfo
                .getRequestorRequestingTimestamp());
        _pendingTimeout =
            _waitingTimeout > remainingCleanupTime ? remainingCleanupTime : _waitingTimeout;
        return newRecord;
      }

      // For users who are not the lock owner and the priority is not higher than current lock
      // owner, throw an exception. The exception will be caught by data accessor, and return
      // false for the update
      LOG.error(
          "User " + _userId + " tried to update the lock at " + new Date(System.currentTimeMillis())
              + ". Lock path: " + _lockPath);

      throw new HelixException("User is not authorized to perform this operation.");
    }
  }

  private ZNRecord composeNewRequestorRecord(LockInfo oldLockInfo, ZNRecord newLockZNRecord) {
    LockInfo lockInfo =
        new LockInfo(oldLockInfo.getOwner(), oldLockInfo.getMessage(), oldLockInfo.getTimeout(),
            oldLockInfo.getPriority(), oldLockInfo.getWaitingTimeout(),
            oldLockInfo.getCleanupTimeout(),
            newLockZNRecord.getSimpleField(LockInfo.LockInfoAttribute.OWNER.name()), newLockZNRecord
            .getIntField(LockInfo.LockInfoAttribute.PRIORITY.name(), DEFAULT_PRIORITY_INT),
            newLockZNRecord.getLongField(LockInfo.LockInfoAttribute.WAITING_TIMEOUT.name(),
                DEFAULT_WAITING_TIMEOUT_LONG), System.currentTimeMillis());
    return lockInfo.getRecord();
  }

  private ZNRecord composeNewOwnerRecord() {
    ZNRecord znRecord = new ZNRecord(ZNODE_ID);
    znRecord.setSimpleField(LockInfo.LockInfoAttribute.OWNER.name(), _userId);
    znRecord.setSimpleField(LockInfo.LockInfoAttribute.MESSAGE.name(), _lockMsg);
    znRecord.setLongField(LockInfo.LockInfoAttribute.TIMEOUT.name(),
        getNonOverflowTimestamp(_leaseTimeout));
    znRecord.setIntField(LockInfo.LockInfoAttribute.PRIORITY.name(), _priority);
    znRecord.setLongField(LockInfo.LockInfoAttribute.WAITING_TIMEOUT.name(), _waitingTimeout);
    znRecord.setLongField(LockInfo.LockInfoAttribute.CLEANUP_TIMEOUT.name(), _cleanupTimeout);
    znRecord.setSimpleField(LockInfo.LockInfoAttribute.REQUESTOR_ID.name(), DEFAULT_REQUESTOR_ID);
    znRecord.setIntField(LockInfo.LockInfoAttribute.REQUESTOR_PRIORITY.name(),
        DEFAULT_REQUESTOR_PRIORITY_INT);
    znRecord.setLongField(LockInfo.LockInfoAttribute.REQUESTOR_WAITING_TIMEOUT.name(),
        DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG);
    znRecord.setLongField(LockInfo.LockInfoAttribute.REQUESTOR_REQUESTING_TIMESTAMP.name(),
        DEFAULT_REQUESTOR_REQUESTING_TIMESTAMP_LONG);
    return znRecord;
  }

  private long getNonOverflowTimestamp(Long timePeriod) {
    if (timePeriod > Long.MAX_VALUE - System.currentTimeMillis()) {
      return Long.MAX_VALUE;
    } else {
      return System.currentTimeMillis() + timePeriod;
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
    private int _priority;
    private long _waitingTimeout;
    private long _cleanupTimeout;
    private boolean _isForceful;
    private LockListener _lockListener;

    public Builder() {
    }

    public Builder setLockScope(LockScope lockScope) {
      _lockScope = lockScope;
      return this;
    }

    public Builder setUserId(String userId) {
      _userId = userId;
      return this;
    }

    public Builder setTimeout(long timeout) {
      _timeout = timeout;
      return this;
    }

    public Builder setLockMsg(String lockMsg) {
      _lockMsg = lockMsg;
      return this;
    }

    public Builder setPriority(int priority) {
      _priority = priority;
      return this;
    }

    public Builder setWaitingTimeout(long waitingTimeout) {
      _waitingTimeout = waitingTimeout;
      return this;
    }

    public Builder setCleanupTimeout(long cleanupTimeout) {
      _cleanupTimeout = cleanupTimeout;
      return this;
    }

    public Builder setIsForceful(boolean isForceful) {
      _isForceful = isForceful;
      return this;
    }

    public Builder setLockListener(LockListener lockListener) {
      _lockListener = lockListener;
      return this;
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
          _priority, _waitingTimeout, _cleanupTimeout, _isForceful, _lockListener,
          baseDataAccessor);
    }
  }
}

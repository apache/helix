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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private LockConstants.LockStatus _lockStatus;
  private long _pendingTimeout;
  private CountDownLatch _countDownLatch = new CountDownLatch(1);

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
    this(scope.getPath(), leaseTimeout, lockMsg, userId, 0, Integer.MAX_VALUE, 0, false, null,
        new ZkBaseDataAccessor<ZNRecord>(zkAddress));
  }

  /**
   * Initialize the lock with ZKLockConfig. This is the preferred way to construct the lock.
   */
  public ZKDistributedNonblockingLock(ZKLockConfig zkLockConfig) {
    this(zkLockConfig.getLockScope(), zkLockConfig.getZkAddress(), zkLockConfig.getLeaseTimeout(),
        zkLockConfig.getLockMsg(), zkLockConfig.getUserId(), zkLockConfig.getPriority(),
        zkLockConfig.getWaitingTimeout(), zkLockConfig.getCleanupTimeout(),
        zkLockConfig.getIsForceful(), zkLockConfig.getLockListener());
  }

  /**
   * Initialize the lock with user provided information, e.g.,cluster, scope, priority,
   * different kinds of timeout, etc.
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
  private ZKDistributedNonblockingLock(LockScope scope, String zkAddress, Long leaseTimeout,
      String lockMsg, String userId, int priority, long waitingTimeout, long cleanupTimeout,
      boolean isForceful, LockListener lockListener) {
    this(scope.getPath(), leaseTimeout, lockMsg, userId, priority, waitingTimeout, cleanupTimeout,
        isForceful, lockListener, new ZkBaseDataAccessor<ZNRecord>(zkAddress));
  }

  /**
   * Internal construction of the lock with user provided information, e.g., lock path under
   * zookeeper, etc.
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
    validateInput();
  }

  @Override
  public boolean tryLock() {
    // Set lock information fields
    _baseDataAccessor.subscribeDataChanges(_lockPath, this);
    LockUpdater updater = new LockUpdater(
        new LockInfo(_userId, _lockMsg, getNonOverflowTimestamp(_leaseTimeout), _priority,
            _waitingTimeout, _cleanupTimeout, null, 0, 0, 0));
    boolean updateResult = _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);

    // Immediately return if the lock statue is not PENDING.
    if (_lockStatus != LockConstants.LockStatus.PENDING) {
      if (!updateResult) {
        _baseDataAccessor.unsubscribeDataChanges(_lockPath, this);
      }
      return updateResult;
    }

    // When the lock status is still pending, wait for the period recorded in _pendingTimeout.
    try {
      _countDownLatch.await(_pendingTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new HelixException(
          String.format("Interruption happened while %s is waiting for the lock", _userId), e);
    }

    // Note the following checks need to be ordered in the current way. Reordering the sequence of
    // checks would cause problem.
    if (_lockStatus != LockConstants.LockStatus.LOCKED) {
      // If the reason for not being able to acquire the lock is due to high priority lock
      // preemption, directly return false.
      if (_lockStatus == LockConstants.LockStatus.PREEMPTED) {
        _baseDataAccessor.unsubscribeDataChanges(_lockPath, this);
        return false;
      }
      // Forceful lock request will grab the lock even the current owner has not finished
      // cleanup work, while non forceful lock request will get an exception.
      if (_isForceful) {
        ZNRecord znRecord = composeNewOwnerRecord();
        ForcefulUpdater forcefulUpdater = new ForcefulUpdater(new LockInfo(znRecord));
        LOG.info("Updating Zookeeper with new owner {} information", _userId);
        _baseDataAccessor.update(_lockPath, forcefulUpdater, AccessOption.PERSISTENT);
        return true;
      } else {
        _baseDataAccessor.unsubscribeDataChanges(_lockPath, this);
        throw new HelixException("Cleanup has not been finished by lock owner");
      }
    }
    return true;
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
    _baseDataAccessor.unsubscribeDataChanges(_lockPath, this);
    _baseDataAccessor.close();
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
    Stat stat = new Stat();
    ZNRecord readData =
        _baseDataAccessor.get(dataPath, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST);
    LockInfo lockInfo = new LockInfo(readData);
    // We are the current owner
    if (isCurrentOwner(lockInfo)) {
      if (lockInfo.getRequestorId().equals(LockConstants.DEFAULT_USER_ID)
          || lockInfo.getPriority() > lockInfo.getRequestorPriority()) {
        LOG.debug("We do not need to handle this data change");
      } else {
        // TODO: ideally we should only start to time for the cleanup action once it starts for
        //  accuracy.
        _lockListener.onCleanupNotification();
        CleanupUpdater cleanupUpdater = new CleanupUpdater();
        boolean res = _baseDataAccessor.update(_lockPath, cleanupUpdater, AccessOption.PERSISTENT);
        if (!res) {
          throw new HelixException(
              String.format("User %s failed to update lock path %s", _userId, _lockPath));
        }
      }
    } // We are the current requestor
    else if (lockInfo.getRequestorId().equals(_userId)) {
      // In case the owner field is empty, it means previous owner has finished cleanup work.
      if (lockInfo.getOwner().equals(LockConstants.DEFAULT_USER_ID)) {
        ZNRecord znRecord = composeNewOwnerRecord();
        LockUpdater updater = new LockUpdater(new LockInfo(znRecord));
        _baseDataAccessor.update(_lockPath, updater, AccessOption.PERSISTENT);
        onAcquiredLockNotification();
      } else {
        LOG.info("We do not need to handle this data change");
      }
    } // If we are waiting for the lock, but find we are not the requestor any more, meaning we
    // are preempted by an even higher priority request
    else if (!lockInfo.getRequestorId().equals(LockConstants.DEFAULT_USER_ID) && !lockInfo
        .getRequestorId().equals(_userId) && _lockStatus == LockConstants.LockStatus.PENDING) {
      onDeniedPendingLockNotification();
    }
  }

  /**
   * call back called when the lock is acquired
   */
  public void onAcquiredLockNotification() {
    _lockStatus = LockConstants.LockStatus.LOCKED;
    _countDownLatch.countDown();
  }

  /**
   * call back called when the pending request is denied due to another higher priority request
   */
  public void onDeniedPendingLockNotification() {
    _lockStatus = LockConstants.LockStatus.PREEMPTED;
    _countDownLatch.countDown();
  }

  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    // TODO: add logic here once we support lock znode cleanup
  }

  /**
   * Class that specifies how a lock node should be updated with another lock node during a
   * trylock/unlock request
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
      if (System.currentTimeMillis() > curLockInfo.getTimeout() || isCurrentOwner(curLockInfo)) {
        return _record;
      }

      // higher priority lock request will try to  preempt current lock owner
      if (!isCurrentOwner(curLockInfo) && _priority > curLockInfo.getPriority()) {
        // if requestor field is empty, fill the field with requestor's id
        if (curLockInfo.getRequestorId().equals(LockConstants.DEFAULT_USER_ID)) {
          _pendingTimeout =
              _waitingTimeout > curLockInfo.getCleanupTimeout() ? curLockInfo.getCleanupTimeout()
                  : _waitingTimeout;
        } // If the requestor field is not empty, and the coming lock request has an even higher
        // priority. The new request will replace current requestor field of the lock
        else if (_priority > curLockInfo.getRequestorPriority()) {
          long remainingCleanupTime =
              curLockInfo.getCleanupTimeout() - (System.currentTimeMillis() - curLockInfo
                  .getRequestingTimestamp());
          _pendingTimeout =
              _waitingTimeout > remainingCleanupTime ? remainingCleanupTime : _waitingTimeout;
        }
        // update lock Znode with requestor information
        ZNRecord newRecord = composeNewRequestorRecord(curLockInfo, _record);
        _lockStatus = LockConstants.LockStatus.PENDING;
        return newRecord;
      }

      // For users who are not the lock owner and the priority is not higher than current lock
      // owner, or the priority is higher than current lock, but lower than the requestor, throw
      // an exception. The exception will be caught by data accessor, and return false for the
      // update operation.
      LOG.error("User {} failed to acquire lock at Lock path {}.", _userId, _lockPath);
      throw new HelixException(
          String.format("User %s failed to acquire lock at Lock path %s.", _userId, _lockPath));
    }
  }

  /**
   * Class that specifies how a lock node should be updated after the previous owner finishes the
   * cleanup work
   */
  private class CleanupUpdater implements DataUpdater<ZNRecord> {
    public CleanupUpdater() {
    }

    @Override
    public ZNRecord update(ZNRecord current) {
      // If we are still the lock owner, clean owner field.
      LockInfo curLockInfo = new LockInfo(current);
      if (isCurrentOwner(curLockInfo)) {
        ZNRecord record = current;
        record
            .setSimpleField(LockInfo.LockInfoAttribute.OWNER.name(), LockConstants.DEFAULT_USER_ID);
        record.setSimpleField(LockInfo.LockInfoAttribute.MESSAGE.name(),
            LockConstants.DEFAULT_MESSAGE_TEXT);
        record.setLongField(LockInfo.LockInfoAttribute.TIMEOUT.name(),
            LockConstants.DEFAULT_TIMEOUT_LONG);
        record.setIntField(LockInfo.LockInfoAttribute.PRIORITY.name(),
            LockConstants.DEFAULT_PRIORITY_INT);
        record.setLongField(LockInfo.LockInfoAttribute.WAITING_TIMEOUT.name(),
            LockConstants.DEFAULT_WAITING_TIMEOUT_LONG);
        record.setLongField(LockInfo.LockInfoAttribute.CLEANUP_TIMEOUT.name(),
            LockConstants.DEFAULT_CLEANUP_TIMEOUT_LONG);
        return record;
      }
      LOG.error("User {} is not current lock owner, and does not need to unlock {}", _userId,
          _lockPath);
      throw new HelixException(String
          .format("User %s is not current lock owner, and does not need" + " to unlock %s", _userId,
              _lockPath));
    }
  }

  /**
   * Class that specifies how a lock node should be updated during a forceful get lock operation
   */
  private class ForcefulUpdater implements DataUpdater<ZNRecord> {
    final ZNRecord _record;

    /**
     * Initialize a structure for lock user to update a lock node value
     * @param lockInfo the lock node value will be used to update the lock
     */
    public ForcefulUpdater(LockInfo lockInfo) {
      _record = lockInfo.getRecord();
    }

    @Override
    public ZNRecord update(ZNRecord current) {
      // If we are still the lock requestor, change ourselves to be lock owner.
      LockInfo curLockInfo = new LockInfo(current);
      if (curLockInfo.getRequestorId().equals(_userId)) {
        return _record;
      }
      LOG.error("User {} is not current lock requestor, and cannot forcefully acquire the lock at "
          + "{}", _userId, _lockPath);
      throw new HelixException(String.format("User %s is not current lock requestor, and cannot "
          + "forcefully acquire the lock at %s", _userId, _lockPath));
    }
  }

  private ZNRecord composeNewRequestorRecord(LockInfo existingLockinfo,
      ZNRecord requestorLockZNRecord) {
    LockInfo lockInfo = new LockInfo(existingLockinfo.getOwner(), existingLockinfo.getMessage(),
        existingLockinfo.getTimeout(), existingLockinfo.getPriority(),
        existingLockinfo.getWaitingTimeout(), existingLockinfo.getCleanupTimeout(),
        requestorLockZNRecord.getSimpleField(LockInfo.LockInfoAttribute.OWNER.name()),
        requestorLockZNRecord.getIntField(LockInfo.LockInfoAttribute.PRIORITY.name(),
            LockConstants.DEFAULT_PRIORITY_INT), requestorLockZNRecord
        .getLongField(LockInfo.LockInfoAttribute.WAITING_TIMEOUT.name(),
            LockConstants.DEFAULT_WAITING_TIMEOUT_LONG), System.currentTimeMillis());
    return lockInfo.getRecord();
  }

  private ZNRecord composeNewOwnerRecord() {
    LockInfo lockInfo =
        new LockInfo(_userId, _lockMsg, getNonOverflowTimestamp(_leaseTimeout), _priority,
            _waitingTimeout, _cleanupTimeout, LockConstants.DEFAULT_USER_ID,
            LockConstants.DEFAULT_PRIORITY_INT, LockConstants.DEFAULT_WAITING_TIMEOUT_LONG,
            LockConstants.DEFAULT_REQUESTING_TIMESTAMP_LONG);
    return lockInfo.getRecord();
  }

  private long getNonOverflowTimestamp(Long timePeriod) {
    if (timePeriod > Long.MAX_VALUE - System.currentTimeMillis()) {
      return Long.MAX_VALUE;
    } else {
      return System.currentTimeMillis() + timePeriod;
    }
  }

  private boolean isCurrentOwner(LockInfo lockInfo) {
    return lockInfo.getOwner().equals(_userId) && (System.currentTimeMillis() < lockInfo
        .getTimeout());
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

  private void validateInput() {
    if (_lockPath == null) {
      throw new IllegalArgumentException("Lock scope cannot be null");
    }
    if (_userId == null) {
      throw new IllegalArgumentException("Owner Id cannot be null");
    }
    if (_leaseTimeout < 0 || _waitingTimeout < 0 || _cleanupTimeout < 0) {
      throw new IllegalArgumentException("Timeout cannot be negative.");
    }
    if (_priority < 0) {
      throw new IllegalArgumentException("Priority cannot be negative.");
    }
  }
}

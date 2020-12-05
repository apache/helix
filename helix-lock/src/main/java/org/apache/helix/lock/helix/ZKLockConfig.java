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

import org.apache.helix.lock.LockScope;
import org.apache.helix.manager.zk.GenericZkHelixApiBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Hold configs used for a ZK distributed nonblocking lock.
 */
public class ZKLockConfig {
  private static final Logger LOG =
      LoggerFactory.getLogger(org.apache.helix.lock.helix.ZKLockConfig.class.getName());
  private LockScope _lockScope;
  private String _zkAddress;
  private Long _leaseTimeout;
  private String _lockMsg;
  private String _userId;
  private int _priority;
  private long _waitingTimeout;
  private long _cleanupTimeout;
  private boolean _isForceful;
  private LockListener _lockListener;

  private ZKLockConfig(LockScope lockScope, String zkAddress, Long leaseTimeout, String lockMsg,
      String userId, int priority, long waitingTimeout, long cleanupTimeout, boolean isForceful,
      LockListener lockListener) {
    _lockScope = lockScope;
    _zkAddress = zkAddress;
    _leaseTimeout = leaseTimeout;
    _lockMsg = lockMsg;
    _userId = userId;
    _priority = priority;
    _waitingTimeout = waitingTimeout;
    _cleanupTimeout = cleanupTimeout;
    _lockListener = lockListener;
    _isForceful = isForceful;
  }

  public LockScope getLockScope() {
    return _lockScope;
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public Long getLeaseTimeout() {
    return _leaseTimeout;
  }

  public String getLockMsg() {
    return _lockMsg;
  }

  public String getUserId() {
    return _userId;
  }

  public int getPriority() {
    return _priority;
  }

  public long getWaitingTimeout() {
    return _waitingTimeout;
  }

  public long getCleanupTimeout() {
    return _cleanupTimeout;
  }

  public boolean getIsForceful() {
    return _isForceful;
  }

  public LockListener getLockListener() {
    return _lockListener;
  }

  /**
   * Builder class to use with ZKLockConfig.
   */
  public static class Builder extends GenericZkHelixApiBuilder<ZKLockConfig.Builder> {
    private LockScope _lockScope;
    private String _zkAddress;
    private long _leaseTimeout;
    private String _lockMsg;
    private String _userId;
    private int _priority;
    private long _waitingTimeout;
    private long _cleanupTimeout;
    private boolean _isForceful;
    private LockListener _lockListener;

    public Builder() {
    }

    public ZKLockConfig build() {
      return new ZKLockConfig(_lockScope, _zkAddress, _leaseTimeout, _lockMsg, _userId, _priority,
          _waitingTimeout, _cleanupTimeout, _isForceful, _lockListener);
    }

    public ZKLockConfig.Builder setLockScope(LockScope lockScope) {
      _lockScope = lockScope;
      return this;
    }

    public ZKLockConfig.Builder setZkAdress(String zkAddress) {
      _zkAddress = zkAddress;
      return this;
    }

    public ZKLockConfig.Builder setUserId(String userId) {
      _userId = userId;
      return this;
    }

    public ZKLockConfig.Builder setLeaseTimeout(long leaseTimeout) {
      _leaseTimeout = leaseTimeout;
      return this;
    }

    public ZKLockConfig.Builder setLockMsg(String lockMsg) {
      _lockMsg = lockMsg;
      return this;
    }

    public ZKLockConfig.Builder setPriority(int priority) {
      _priority = priority;
      return this;
    }

    public ZKLockConfig.Builder setWaitingTimeout(long waitingTimeout) {
      _waitingTimeout = waitingTimeout;
      return this;
    }

    public ZKLockConfig.Builder setCleanupTimeout(long cleanupTimeout) {
      _cleanupTimeout = cleanupTimeout;
      return this;
    }

    public ZKLockConfig.Builder setIsForceful(boolean isForceful) {
      _isForceful = isForceful;
      return this;
    }

    public ZKLockConfig.Builder setLockListener(LockListener lockListener) {
      _lockListener = lockListener;
      return this;
    }
  }
}

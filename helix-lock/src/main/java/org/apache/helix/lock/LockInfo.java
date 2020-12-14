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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.lock.helix.LockConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Structure represents a lock node information, implemented using ZNRecord
 */
public class LockInfo {
  // default lock info represents the status of a unlocked lock
  public static final LockInfo defaultLockInfo =
      new LockInfo();

  public static final String ZNODE_ID = "LOCK";
  private ZNRecord _record;

  /**
   * The keys to lock information
   */
  public enum LockInfoAttribute {
    OWNER,
    MESSAGE,
    TIMEOUT,
    PRIORITY,
    WAITING_TIMEOUT,
    CLEANUP_TIMEOUT,
    REQUESTOR_ID,
    REQUESTOR_PRIORITY,
    REQUESTOR_WAITING_TIMEOUT,
    REQUESTING_TIMESTAMP
  }

  /**
   * Initialize a default LockInfo instance
   */
  private LockInfo() {
    setLockInfoFields(LockConstants.DEFAULT_USER_ID, LockConstants.DEFAULT_MESSAGE_TEXT,
        LockConstants.DEFAULT_TIMEOUT_LONG, LockConstants.DEFAULT_PRIORITY_INT,
        LockConstants.DEFAULT_WAITING_TIMEOUT_LONG, LockConstants.DEFAULT_CLEANUP_TIMEOUT_LONG,
        LockConstants.DEFAULT_USER_ID, LockConstants.DEFAULT_PRIORITY_INT,
        LockConstants.DEFAULT_WAITING_TIMEOUT_LONG,
        LockConstants.DEFAULT_REQUESTING_TIMESTAMP_LONG);
  }

  /**
   * Initialize a LockInfo with a ZNRecord, set all info fields to default data
   * @param znRecord The ZNRecord contains lock node data that used to initialize the LockInfo
   */
  public LockInfo(ZNRecord znRecord) {
    if (znRecord == null) {
      znRecord = new ZNRecord(ZNODE_ID);
    }
    String ownerId = znRecord.getSimpleField(LockInfoAttribute.OWNER.name()) == null
        ? LockConstants.DEFAULT_USER_ID : znRecord.getSimpleField(LockInfoAttribute.OWNER.name());
    String message = znRecord.getSimpleField(LockInfoAttribute.MESSAGE.name()) == null
        ? LockConstants.DEFAULT_MESSAGE_TEXT
        : znRecord.getSimpleField(LockInfoAttribute.MESSAGE.name());
    long timeout =
        znRecord.getLongField(LockInfoAttribute.TIMEOUT.name(), LockConstants.DEFAULT_TIMEOUT_LONG);
    int priority =
        znRecord.getIntField(LockInfoAttribute.PRIORITY.name(), LockConstants.DEFAULT_PRIORITY_INT);
      long waitingTimeout = znRecord.getLongField(LockInfoAttribute.WAITING_TIMEOUT.name(),
          LockConstants.DEFAULT_WAITING_TIMEOUT_LONG);
      long cleanupTimeout = znRecord.getLongField(LockInfoAttribute.CLEANUP_TIMEOUT.name(),
          LockConstants.DEFAULT_CLEANUP_TIMEOUT_LONG);
      String requestorId = znRecord.getSimpleField(LockInfoAttribute.REQUESTOR_ID.name());
      int requestorPriority = znRecord.getIntField(LockInfoAttribute.REQUESTOR_PRIORITY.name(),
          LockConstants.DEFAULT_PRIORITY_INT);
      long requestorWaitingTimeout = znRecord
          .getLongField(LockInfoAttribute.REQUESTOR_WAITING_TIMEOUT.name(),
              LockConstants.DEFAULT_WAITING_TIMEOUT_LONG);
      long requestingTimestamp = znRecord
          .getLongField(LockInfoAttribute.REQUESTING_TIMESTAMP.name(),
              LockConstants.DEFAULT_REQUESTING_TIMESTAMP_LONG);
      setLockInfoFields(ownerId, message, timeout, priority, waitingTimeout, cleanupTimeout,
          requestorId, requestorPriority, requestorWaitingTimeout, requestingTimestamp);
  }

  /**
   * Initialize a LockInfo with data for each field, set all null info fields to default data.
   * @param ownerId value of OWNER attribute
   * @param message value of MESSAGE attribute
   * @param timeout value of TIMEOUT attribute
   * @param priority value of PRIORITY attribute
   * @param waitingTimout value of WAITING_TIMEOUT attribute
   * @param cleanupTimeout value of CLEANUP_TIMEOUT attribute
   * @param requestorId value of REQUESTOR_ID attribute
   * @param requestorPriority value of REQUESTOR_PRIORITY attribute
   * @param requestorWaitingTimeout value of REQUESTOR_WAITING_TIMEOUT attribute
   * @param requestingTimestamp value of REQUESTING_TIMESTAMP attribute
   */
  public LockInfo(String ownerId, String message, long timeout, int priority, long waitingTimout,
      long cleanupTimeout, String requestorId, int requestorPriority, long requestorWaitingTimeout,
      long requestingTimestamp) {
    setLockInfoFields(ownerId, message, timeout, priority, waitingTimout, cleanupTimeout,
        requestorId, requestorPriority, requestorWaitingTimeout, requestingTimestamp);
  }

  /**
   * Set each field of lock info to user provided values if the values are not null. Null values
   * are set to default values.
   */
  private void setLockInfoFields(String ownerId, String message, long timeout, int priority,
      long waitingTimeout, long cleanupTimeout, String requestorId, int requestorPriority,
      long requestorWaitingTimeout, long requestingTimestamp) {
    _record = new ZNRecord(ZNODE_ID);
    _record.setSimpleField(LockInfoAttribute.OWNER.name(),
        ownerId == null ? LockConstants.DEFAULT_USER_ID : ownerId);
    _record.setSimpleField(LockInfoAttribute.MESSAGE.name(),
        message == null ? LockConstants.DEFAULT_MESSAGE_TEXT : message);
    _record.setLongField(LockInfoAttribute.TIMEOUT.name(), timeout);
    _record.setIntField(LockInfoAttribute.PRIORITY.name(), priority);
    _record.setLongField(LockInfoAttribute.WAITING_TIMEOUT.name(), waitingTimeout);
    _record.setLongField(LockInfoAttribute.CLEANUP_TIMEOUT.name(), cleanupTimeout);
    _record.setSimpleField(LockInfoAttribute.REQUESTOR_ID.name(), requestorId);
    _record.setIntField(LockInfoAttribute.REQUESTOR_PRIORITY.name(), requestorPriority);
    _record
        .setLongField(LockInfoAttribute.REQUESTOR_WAITING_TIMEOUT.name(), requestorWaitingTimeout);
    _record.setLongField(LockInfoAttribute.REQUESTING_TIMESTAMP.name(), requestingTimestamp);
  }

  /**
   * Get the value for OWNER attribute of the lock
   * @return the owner id of the lock, empty string if there is no owner id set
   */
  public String getOwner() {
    String owner = _record.getSimpleField(LockInfoAttribute.OWNER.name());
    return owner == null ? LockConstants.DEFAULT_USER_ID : owner;
  }

  /**
   * Get the value for MESSAGE attribute of the lock
   * @return the message of the lock, empty string if there is no message set
   */
  public String getMessage() {
    String message = _record.getSimpleField(LockInfoAttribute.MESSAGE.name());
    return message == null ? LockConstants.DEFAULT_MESSAGE_TEXT : message;
  }

  /**
   * Get the value for TIMEOUT attribute of the lock
   * @return the expiration timestamp of the lock, -1 if there is no timeout set
   */
  public Long getTimeout() {
    return _record
        .getLongField(LockInfoAttribute.TIMEOUT.name(), LockConstants.DEFAULT_TIMEOUT_LONG);
  }

  /**
   * Get the value for PRIORITY attribute of the lock
   * @return the priority of the lock, -1 if there is no priority set
   */
  public Integer getPriority() {
    return _record
        .getIntField(LockInfoAttribute.PRIORITY.name(), LockConstants.DEFAULT_PRIORITY_INT);
  }

  /**
   * Get the value for WAITING_TIMEOUT attribute of the lock
   * @return the waiting timeout of the lock, -1 if there is no waiting timeout set
   */
  public Long getWaitingTimeout() {
    return _record.getLongField(LockInfoAttribute.WAITING_TIMEOUT.name(),
        LockConstants.DEFAULT_WAITING_TIMEOUT_LONG);
  }

  /**
   * Get the value for CLEANUP_TIMEOUT attribute of the lock
   * @return the cleanup time of the lock, -1 if there is no cleanup timeout set
   */
  public Long getCleanupTimeout() {
    return _record.getLongField(LockInfoAttribute.CLEANUP_TIMEOUT.name(),
        LockConstants.DEFAULT_CLEANUP_TIMEOUT_LONG);
  }

  /**
   * Get the value for REQUESTOR_ID attribute of the lock
   * @return the requestor id of the lock, -1 if there is no requestor id set
   */
  public String getRequestorId() {
    String requestorId = _record.getSimpleField(LockInfoAttribute.REQUESTOR_ID.name());
    return requestorId == null ? LockConstants.DEFAULT_USER_ID : requestorId;
  }

  /**
   * Get the value for REQUESTOR_PRIORITY attribute of the lock
   * @return the requestor priority of the lock, -1 if there is no requestor priority set
   */
  public int getRequestorPriority() {
    return _record.getIntField(LockInfoAttribute.REQUESTOR_PRIORITY.name(),
        LockConstants.DEFAULT_PRIORITY_INT);
  }

  /**
   * Get the value for REQUESTOR_WAITING_TIMEOUT attribute of the lock
   * @return the requestor waiting timeout of the lock, -1 if there is no requestor timeout set
   */
  public long getRequestorWaitingTimeout() {
    return _record.getLongField(LockInfoAttribute.REQUESTOR_WAITING_TIMEOUT.name(),
        LockConstants.DEFAULT_WAITING_TIMEOUT_LONG);
  }

  /**
   * Get the value for REQUESTOR_REQUESTING_TIMESTAMP attribute of the lock
   * @return the requestor requesting timestamp of the lock, -1 if there is no requestor
   * requestingi timestamp set
   */
  public long getRequestingTimestamp() {
    return _record.getLongField(LockInfoAttribute.REQUESTING_TIMESTAMP.name(),
        LockConstants.DEFAULT_WAITING_TIMEOUT_LONG);
  }

  /**
   * Get the underlying ZNRecord in a LockInfo
   * @return lock information contained in a ZNRecord
   */
  public ZNRecord getRecord() {
    return _record;
  }
}

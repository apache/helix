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

import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Structure represents a lock node information, implemented using ZNRecord
 */
public class LockInfo {

  // Default values for each attribute if there are no current values set by user
  public static final String DEFAULT_OWNER_TEXT = "";
  public static final String DEFAULT_MESSAGE_TEXT = "";
  public static final long DEFAULT_TIMEOUT_LONG = -1;
  public static final int DEFAULT_PRIORITY_INT = -1;
  public static final long DEFAULT_WAITING_TIMEOUT_LONG = -1;
  public static final long DEFAULT_CLEANUP_TIMEOUT_LONG = -1;
  public static final String DEFAULT_REQUESTOR_ID = "";
  public static final int DEFAULT_REQUESTOR_PRIORITY_INT = -1;
  public static final long DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG = -1;
  public static final long DEFAULT_REQUESTOR_REQUESTING_TIMESTAMP_LONG = -1;

  // default lock info represents the status of a unlocked lock
  public static final LockInfo defaultLockInfo =
      new LockInfo(DEFAULT_OWNER_TEXT, DEFAULT_MESSAGE_TEXT, DEFAULT_TIMEOUT_LONG,
          DEFAULT_PRIORITY_INT, DEFAULT_WAITING_TIMEOUT_LONG, DEFAULT_CLEANUP_TIMEOUT_LONG,
          DEFAULT_REQUESTOR_ID, DEFAULT_REQUESTOR_PRIORITY_INT,
          DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG, DEFAULT_REQUESTOR_REQUESTING_TIMESTAMP_LONG);

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
    REQUESTOR_REQUESTING_TIMESTAMP
  }

  /**
   * Initialize a default LockInfo instance
   */
  private LockInfo() {
    _record = new ZNRecord(ZNODE_ID);
    setLockInfoFields(DEFAULT_OWNER_TEXT, DEFAULT_MESSAGE_TEXT, DEFAULT_TIMEOUT_LONG,
        DEFAULT_PRIORITY_INT, DEFAULT_WAITING_TIMEOUT_LONG, DEFAULT_CLEANUP_TIMEOUT_LONG,
        DEFAULT_REQUESTOR_ID, DEFAULT_REQUESTOR_PRIORITY_INT,
        DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG, DEFAULT_REQUESTOR_REQUESTING_TIMESTAMP_LONG);
  }

  /**
   * Initialize a LockInfo with a ZNRecord, set all info fields to default data
   * @param znRecord The ZNRecord contains lock node data that used to initialize the LockInfo
   */
  public LockInfo(ZNRecord znRecord) {
    this();
    if (znRecord != null) {
      String ownerId = znRecord.getSimpleField(LockInfoAttribute.OWNER.name());
      String message = znRecord.getSimpleField(LockInfoAttribute.MESSAGE.name());
      long timeout = znRecord.getLongField(LockInfoAttribute.TIMEOUT.name(), DEFAULT_TIMEOUT_LONG);
      int priority = znRecord.getIntField(LockInfoAttribute.PRIORITY.name(), DEFAULT_PRIORITY_INT);
      long waitingTimeout = znRecord
          .getLongField(LockInfoAttribute.WAITING_TIMEOUT.name(), DEFAULT_WAITING_TIMEOUT_LONG);
      long cleanupTimeout = znRecord
          .getLongField(LockInfoAttribute.CLEANUP_TIMEOUT.name(), DEFAULT_CLEANUP_TIMEOUT_LONG);
      String requestorId = znRecord.getSimpleField(LockInfoAttribute.REQUESTOR_ID.name());
      int requestorPriority = znRecord
          .getIntField(LockInfoAttribute.REQUESTOR_PRIORITY.name(), DEFAULT_REQUESTOR_PRIORITY_INT);
      long requestorWaitingTimeout = znRecord
          .getLongField(LockInfoAttribute.REQUESTOR_WAITING_TIMEOUT.name(),
              DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG);
      long requestorRequestingTimestamp = znRecord
          .getLongField(LockInfoAttribute.REQUESTOR_REQUESTING_TIMESTAMP.name(),
              DEFAULT_REQUESTOR_REQUESTING_TIMESTAMP_LONG);
      setLockInfoFields(ownerId, message, timeout, priority, waitingTimeout, cleanupTimeout,
          requestorId, requestorPriority, requestorWaitingTimeout, requestorRequestingTimestamp);
    }
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
   * @param requestorRequestingTimestamp value of REQUESTOR_REQUESTING_TIMESTAMP attribute
   */
  public LockInfo(String ownerId, String message, long timeout, int priority, long waitingTimout,
      long cleanupTimeout, String requestorId, int requestorPriority, long requestorWaitingTimeout,
      long requestorRequestingTimestamp) {
    this();
    setLockInfoFields(ownerId, message, timeout, priority, waitingTimout, cleanupTimeout,
        requestorId, requestorPriority, requestorWaitingTimeout, requestorRequestingTimestamp);
  }

  /**
   * Set each field of lock info to user provided values if the values are not null. Null values
   * are set to default values.
   */
  private void setLockInfoFields(String ownerId, String message, long timeout, int priority,
      long waitingTimeout, long cleanupTimeout, String requestorId, int requestorPriority,
      long requestorWaitingTimeout, long requestorRequestingTimestamp) {
    _record.setSimpleField(LockInfoAttribute.OWNER.name(),
        ownerId == null ? DEFAULT_OWNER_TEXT : ownerId);
    _record.setSimpleField(LockInfoAttribute.MESSAGE.name(),
        message == null ? DEFAULT_MESSAGE_TEXT : message);
    _record.setLongField(LockInfoAttribute.TIMEOUT.name(), timeout);
    _record.setIntField(LockInfoAttribute.PRIORITY.name(), priority);
    _record.setLongField(LockInfoAttribute.WAITING_TIMEOUT.name(), waitingTimeout);
    _record.setLongField(LockInfoAttribute.CLEANUP_TIMEOUT.name(), cleanupTimeout);
    _record.setSimpleField(LockInfoAttribute.REQUESTOR_ID.name(), requestorId);
    _record.setIntField(LockInfoAttribute.REQUESTOR_PRIORITY.name(), requestorPriority);
    _record
        .setLongField(LockInfoAttribute.REQUESTOR_WAITING_TIMEOUT.name(), requestorWaitingTimeout);
    _record.setLongField(LockInfoAttribute.REQUESTOR_REQUESTING_TIMESTAMP.name(),
        requestorRequestingTimestamp);
  }

  /**
   * Get the value for OWNER attribute of the lock
   * @return the owner id of the lock, empty string if there is no owner id set
   */
  public String getOwner() {
    String owner = _record.getSimpleField(LockInfoAttribute.OWNER.name());
    return owner == null ? DEFAULT_OWNER_TEXT : owner;
  }

  /**
   * Get the value for MESSAGE attribute of the lock
   * @return the message of the lock, empty string if there is no message set
   */
  public String getMessage() {
    String message = _record.getSimpleField(LockInfoAttribute.MESSAGE.name());
    return message == null ? DEFAULT_MESSAGE_TEXT : message;
  }

  /**
   * Get the value for TIMEOUT attribute of the lock
   * @return the expiration timestamp of the lock, -1 if there is no timeout set
   */
  public Long getTimeout() {
    return _record.getLongField(LockInfoAttribute.TIMEOUT.name(), DEFAULT_TIMEOUT_LONG);
  }

  /**
   * Get the value for PRIORITY attribute of the lock
   * @return the priority of the lock, -1 if there is no priority set
   */
  public Integer getPriority() {
    return _record.getIntField(LockInfoAttribute.PRIORITY.name(), DEFAULT_PRIORITY_INT);
  }

  /**
   * Get the value for WAITING_TIMEOUT attribute of the lock
   * @return the waiting timeout of the lock, -1 if there is no waiting timeout set
   */
  public Long getWaitingTimeout() {
    return _record
        .getLongField(LockInfoAttribute.WAITING_TIMEOUT.name(), DEFAULT_WAITING_TIMEOUT_LONG);
  }

  /**
   * Get the value for CLEANUP_TIMEOUT attribute of the lock
   * @return the cleanup time of the lock, -1 if there is no cleanup timeout set
   */
  public Long getCleanupTimeout() {
    return _record
        .getLongField(LockInfoAttribute.CLEANUP_TIMEOUT.name(), DEFAULT_CLEANUP_TIMEOUT_LONG);
  }

  /**
   * Get the value for REQUESTOR_ID attribute of the lock
   * @return the requestor id of the lock, -1 if there is no requestor id set
   */
  public String getRequestorId() {
    String requestorId = _record.getSimpleField(LockInfoAttribute.REQUESTOR_ID.name());
    return requestorId == null ? DEFAULT_REQUESTOR_ID : requestorId;
  }

  /**
   * Get the value for REQUESTOR_PRIORITY attribute of the lock
   * @return the requestor priority of the lock, -1 if there is no requestor priority set
   */
  public int getRequestorPriority() {
    return _record
        .getIntField(LockInfoAttribute.REQUESTOR_PRIORITY.name(), DEFAULT_REQUESTOR_PRIORITY_INT);
  }

  /**
   * Get the value for REQUESTOR_WAITING_TIMEOUT attribute of the lock
   * @return the requestor waiting timeout of the lock, -1 if there is no requestor timeout set
   */
  public long getRequestorWaitingTimeout() {
    return _record.getLongField(LockInfoAttribute.REQUESTOR_WAITING_TIMEOUT.name(),
        DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG);
  }

  /**
   * Get the value for REQUESTOR_REQUESTING_TIMESTAMP attribute of the lock
   * @return the requestor requesting timestamp of the lock, -1 if there is no requestor
   * requestingi timestamp set
   */
  public long getRequestorRequestingTimestamp() {
    return _record.getLongField(LockInfoAttribute.REQUESTOR_REQUESTING_TIMESTAMP.name(),
        DEFAULT_REQUESTOR_WAITING_TIMEOUT_LONG);
  }

  /**
   * Get the underlying ZNRecord in a LockInfo
   * @return lock information contained in a ZNRecord
   */
  public ZNRecord getRecord() {
    return _record;
  }
}

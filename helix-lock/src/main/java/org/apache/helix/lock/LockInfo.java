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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;


/**
 * Structure represents a lock node information, implemented using ZNRecord
 */
public class LockInfo extends HelixProperty {

  // Default values for each attribute if there are no current values set by user
  public static final String DEFAULT_OWNER_TEXT = "";
  public static final String DEFAULT_MESSAGE_TEXT = "";
  public static final long DEFAULT_TIMEOUT_LONG = -1L;

  // default lock info represents the status of a unlocked lock
  public static final LockInfo defaultLockInfo = new LockInfo("");

  /**
   * The keys to lock information
   */
  public enum LockInfoAttribute {
    OWNER, MESSAGE, TIMEOUT
  }

  /**
   * Initialize a LockInfo with a ZNRecord id, set all info fields to default data
   */
  public LockInfo(String id) {
    super(id);
    resetLockInfo();
  }

  /**
   * Initialize a LockInfo with a ZNRecord, set all info fields to default data
   * @param znRecord The ZNRecord contains lock node data that used to initialize the LockInfo
   */
  public LockInfo(ZNRecord znRecord) {
    super(znRecord);
    setNullLockInfoFieldsToDefault();
  }

  /**
   * Initialize a LockInfo with data for each field, set all null info fields to default data
   * @param ownerId value of OWNER attribute
   * @param message value of MESSAGE attribute
   * @param timeout value of TIMEOUT attribute
   */
  public LockInfo(String ownerId, String message, long timeout) {
    this(ownerId);
    setLockInfoFields(ownerId, message, timeout);
  }

  /**
   * Build a LOCKINFO instance that represents an unlocked lock states
   * @return the unlocked lock node LockInfo instance
   */
  public static LockInfo buildUnlockedLockInfo() {
    return new LockInfo("");
  }

  /**
   * Set each field of lock info to user provided values if the values are not null, null values are set to default values
   * @param ownerId value of OWNER attribute
   * @param message value of MESSAGE attribute
   * @param timeout value of TIMEOUT attribute
   */
  public void setLockInfoFields(String ownerId, String message, Long timeout) {
    _record.setSimpleField(LockInfoAttribute.OWNER.name(),
        ownerId == null ? DEFAULT_OWNER_TEXT : ownerId);
    _record.setSimpleField(LockInfoAttribute.MESSAGE.name(),
        message == null ? DEFAULT_MESSAGE_TEXT : message);
    _record.setLongField(LockInfoAttribute.TIMEOUT.name(),
        timeout == null ? DEFAULT_TIMEOUT_LONG : timeout);
  }

  /**
   * Set all null values to default values in LockInfo, keep non-null values
   */
  private void setNullLockInfoFieldsToDefault() {
    setLockInfoFields(getOwner(), getMessage(), getTimeout());
  }

  /**
   * Reset the lock info to unlocked lock state
   */
  public void resetLockInfo() {
    setLockInfoFields(DEFAULT_OWNER_TEXT, DEFAULT_MESSAGE_TEXT, DEFAULT_TIMEOUT_LONG);
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
   * @return the expiring time of the lock, -1 if there is no timeout set
   */
  public Long getTimeout() {
    return _record.getLongField(LockInfoAttribute.TIMEOUT.name(), DEFAULT_TIMEOUT_LONG);
  }

  /**
   * Get the value for OWNER attribute of the lock from a ZNRecord
   * @return the owner id of the lock, empty string if there is no owner id set
   */
  public static String getOwner(ZNRecord znRecord) {
    if (znRecord == null) {
      return DEFAULT_OWNER_TEXT;
    }
    String owner = znRecord.getSimpleField(LockInfoAttribute.OWNER.name());
    return owner == null ? DEFAULT_OWNER_TEXT : owner;
  }

  /**
   * Get the value for MESSAGE attribute of the lock from a ZNRecord
   * @return the message of the lock, empty string if there is no message set
   */
  public static String getMessage(ZNRecord znRecord) {
    if (znRecord == null) {
      return DEFAULT_MESSAGE_TEXT;
    }
    String message = znRecord.getSimpleField(LockInfoAttribute.MESSAGE.name());
    return message == null ? DEFAULT_MESSAGE_TEXT : message;
  }

  /**
   * Get the value for TIMEOUT attribute of the lock from a ZNRecord
   * @return the expiring time of the lock, -1 if there is no timeout set
   */
  public static long getTimeout(ZNRecord znRecord) {
    if (znRecord == null) {
      return DEFAULT_TIMEOUT_LONG;
    }
    return znRecord.getLongField(LockInfoAttribute.TIMEOUT.name(), DEFAULT_TIMEOUT_LONG);
  }

  /**
   * Check if the lock has a owner id set
   * @return true if an owner id is set, false if not
   */
  public static boolean ownerIdSet(ZNRecord znRecord) {
    String ownerId = getOwner(znRecord);
    return !ownerId.equals(DEFAULT_OWNER_TEXT);
  }
}

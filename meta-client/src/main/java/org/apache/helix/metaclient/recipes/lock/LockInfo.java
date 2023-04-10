package org.apache.helix.metaclient.recipes.lock;

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

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.apache.helix.metaclient.datamodel.DataRecord;


/**
 * This structure represents a Lock node information, implemented using DataRecord
 */
public class LockInfo {

  // Default values for each attribute if there are no current values set by user
  public static final String DEFAULT_LOCK_ID_TEXT = "";
  public static final String DEFAULT_OWNER_ID_TEXT = "";
  public static final String DEFAULT_CLIENT_ID_TEXT = "";
  public static final String DEFAULT_CLIENT_DATA = "";
  public static final long DEFAULT_GRANTED_AT_LONG = -1L;
  public static final long DEFAULT_LAST_RENEWED_AT_LONG = -1L;
  public static final Duration DEFAULT_TIMEOUT_DURATION = Duration.ofMillis(-1L);
  private static final String ZNODE_ID = "LOCK";
  private final DataRecord _record;

  /**
   * The keys to lock information
   */
  public enum LockInfoAttribute {
    LOCK_ID,
    OWNER_ID,
    CLIENT_ID,
    CLIENT_DATA,
    GRANTED_AT,
    LAST_RENEWED_AT,
    TIMEOUT
  }

  /**
   * Initialize a default LockInfo instance
   */
  private LockInfo() {
    _record = new DataRecord(ZNODE_ID);
    setLockInfoFields(DEFAULT_LOCK_ID_TEXT, DEFAULT_OWNER_ID_TEXT, DEFAULT_CLIENT_ID_TEXT, DEFAULT_CLIENT_DATA, DEFAULT_GRANTED_AT_LONG,
        DEFAULT_LAST_RENEWED_AT_LONG, DEFAULT_TIMEOUT_DURATION);
  }

  /**
   * Initialize a LockInfo with a ZNRecord, set all info fields to default data
   * @param dataRecord The dataRecord contains lock node data that used to initialize the LockInfo
   */
  public LockInfo(DataRecord dataRecord) {
    this();
    if (dataRecord != null) {
      String lockId = dataRecord.getSimpleField(LockInfoAttribute.LOCK_ID.name());
      String ownerId = dataRecord.getSimpleField(LockInfoAttribute.OWNER_ID.name());
      String clientId = dataRecord.getSimpleField(LockInfoAttribute.CLIENT_ID.name());
      String clientData = dataRecord.getSimpleField(LockInfoAttribute.CLIENT_DATA.name());
      long grantTime = dataRecord.getLongField(LockInfoAttribute.GRANTED_AT.name(), DEFAULT_GRANTED_AT_LONG);
      long lastRenewalTime =
          dataRecord.getLongField(LockInfoAttribute.LAST_RENEWED_AT.name(), DEFAULT_LAST_RENEWED_AT_LONG);
      long timeout = dataRecord.getLongField(LockInfoAttribute.TIMEOUT.name(), DEFAULT_TIMEOUT_DURATION.toMillis());
      Duration duration = Duration.of(timeout, ChronoUnit.MILLIS);
      setLockInfoFields(lockId,ownerId, clientId, clientData, grantTime,
          lastRenewalTime, duration);
    }
  }

  /**
   * Initialize a LockInfo with data for each field, set all null info fields to default data
   * @param lockId value of LOCK_ID attribute
   * @param ownerId value of OWNER_ID attribute
   * @param clientId value of CLIENT_ID attribute
   * @param clientData value of CLIENT_DATA attribute
   * @param grantTime the time the lock was granted
   * @param lastRenewalTime the last time the lock was renewed
   * @param timeout value of TIMEOUT attribute
   */
  public LockInfo(String lockId, String ownerId, String clientId,
                  String clientData, long grantTime, long lastRenewalTime, Duration timeout) {
    this();
    setLockInfoFields(lockId, ownerId, clientId, clientData, grantTime, lastRenewalTime, timeout);
  }

  /**
   * Set each field of lock info to user provided values if the values
   * are not null, null values are set to default values
   * @param lockId value of LOCK_ID attribute
   * @param ownerId value of OWNER_ID attribute
   * @param clientId value of CLIENT_ID attribute
   * @param clientData value of CLIENT_DATA attribute
   * @param grantTime the time the lock was granted
   * @param lastRenewalTime the last time the lock was renewed
   * @param timeout value of TIMEOUT attribute
   */
  private void setLockInfoFields(String lockId, String ownerId, String clientId, String clientData, long grantTime, long lastRenewalTime,
                                 Duration timeout) {
    setLockId(lockId);
    setOwnerId(ownerId);
    setClientId(clientId);
    setClientData(clientData);
    setGrantedAt(grantTime);
    setLastRenewedAt(lastRenewalTime);
    setTimeout(timeout);
  }

  /**
   * Set the value for LOCK_ID attribute of the lock
   * @param lockId Is a unique identifier representing the lock.
   *               It is created by the lockClient and a new one is created for each time the lock is acquired.
   */
  public void setLockId(String lockId) {
    _record.setSimpleField(LockInfoAttribute.LOCK_ID.name(), lockId == null ? DEFAULT_LOCK_ID_TEXT : lockId);
  }

  /**
   * Get the value for OWNER_ID attribute of the lock
   * @param ownerId Represents the initiator of the lock, created by the client.
   *                A service can have multiple ownerId's as long as acquire and release are called
   *                by the same owner.
   */
  public void setOwnerId(String ownerId) {
    _record.setSimpleField(LockInfoAttribute.OWNER_ID.name(), ownerId == null ? DEFAULT_OWNER_ID_TEXT : ownerId);
  }

  /**
   * Get the value for CLIENT_ID attribute of the lock
   * @param clientId Unique identifier that represents who will get the lock (the client).
   */
  public void setClientId(String clientId) {
    _record.setSimpleField(LockInfoAttribute.CLIENT_ID.name(), clientId == null ? DEFAULT_CLIENT_ID_TEXT : clientId);
  }

  /**
   * Get the value for CLIENT_DATA attribute of the lock
   * @param clientData String representing the serialized data object
   */
  public void setClientData(String clientData) {
    _record.setSimpleField(LockInfoAttribute.CLIENT_DATA.name(), clientData == null ? DEFAULT_CLIENT_DATA : clientData);
  }

  /**
   * Get the value for GRANTED_AT attribute of the lock
   * @param grantTime Long representing the time at which the lock was granted
   */
  public void setGrantedAt(Long grantTime) {
    _record.setLongField(LockInfoAttribute.GRANTED_AT.name(), grantTime);
  }

  /**
   * Get the value for LAST_RENEWED_AT attribute of the lock
   * @param lastRenewalTime Long representing the time at which the lock was last renewed
   */
  public void setLastRenewedAt(Long lastRenewalTime) {
    _record.setLongField(LockInfoAttribute.LAST_RENEWED_AT.name(), lastRenewalTime);
  }

  /**
   * Get the value for TIMEOUT attribute of the lock
   * @param timeout Duration object representing the duration of a lock.
   */
  public void setTimeout(Duration timeout) {
    // Always store the timeout value in milliseconds for the sake of simplicity
    _record.setLongField(LockInfoAttribute.TIMEOUT.name(), timeout.toMillis());
  }

  /**
   * Get the value for OWNER_ID attribute of the lock
   * @return the owner id of the lock, {@link #DEFAULT_OWNER_ID_TEXT} if there is no owner id set
   */
  public String getOwnerId() {
    return _record.getStringField(LockInfoAttribute.OWNER_ID.name(), DEFAULT_OWNER_ID_TEXT);
  }

  /**
   * Get the value for CLIENT_ID attribute of the lock
   * @return the client id of the lock, {@link #DEFAULT_CLIENT_ID_TEXT} if there is no client id set
   */
  public String getClientId() {
    return _record.getStringField(LockInfoAttribute.CLIENT_ID.name(), DEFAULT_CLIENT_ID_TEXT);
  }

  /**
   * Get the value for LOCK_ID attribute of the lock
   * @return the id of the lock, {@link #DEFAULT_LOCK_ID_TEXT} if there is no lock id set
   */
  public String getLockId() {
    return _record.getStringField(LockInfoAttribute.LOCK_ID.name(), DEFAULT_LOCK_ID_TEXT);
  }

  /**
   * Get value of CLIENT_DATA
   * @return the string representing the serialized client data, {@link #DEFAULT_CLIENT_DATA}
   * if there is no client data set.
   */
  public String getClientData() {
    return _record.getStringField(LockInfoAttribute.CLIENT_DATA.name(), DEFAULT_CLIENT_DATA);
  }

  /**
   * Get the time the lock was granted on
   * @return the grant time of the lock, {@link #DEFAULT_GRANTED_AT_LONG}
   * if there is no grant time set
   */
  public Long getGrantedAt() {
    return _record.getLongField(LockInfoAttribute.GRANTED_AT.name(), DEFAULT_GRANTED_AT_LONG);
  }

  /**
   * Get the last time the lock was renewed
   * @return the last renewal time of the lock, {@link #DEFAULT_LAST_RENEWED_AT_LONG}
   * if there is no renewal time set
   */
  public Long getLastRenewedAt() {
    return _record
        .getLongField(LockInfoAttribute.LAST_RENEWED_AT.name(), DEFAULT_LAST_RENEWED_AT_LONG);
  }

  /**
   * Get the value for TIMEOUT attribute of the lock
   * @return the expiring time of the lock, {@link #DEFAULT_TIMEOUT_DURATION} if there is no timeout set
   */
  public Duration getTimeout() {
    long timeout = _record.getLongField(LockInfoAttribute.TIMEOUT.name(), DEFAULT_TIMEOUT_DURATION.toMillis());
    return Duration.of(timeout, ChronoUnit.MILLIS);
  }

  /**
   * Get the underlying DataRecord in a LockInfo
   * @return lock information contained in a DataRecord
   */
  public DataRecord getRecord() {
    return _record;
  }
}

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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.datamodel.DataRecord;


/**
 * This structure represents a Lock node information, implemented using DataRecord
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class LockInfo extends DataRecord {

  // Default values for each attribute if there are no current values set by user
  public static final String DEFAULT_LOCK_ID_TEXT = "";
  public static final String DEFAULT_OWNER_ID_TEXT = "";
  public static final String DEFAULT_CLIENT_ID_TEXT = "";
  public static final String DEFAULT_CLIENT_DATA = "";
  public static final long DEFAULT_GRANTED_AT_LONG = -1L;
  public static final long DEFAULT_LAST_RENEWED_AT_LONG = -1L;
  public static final long DEFAULT_TIMEOUT_DURATION = -1L;
  private static final String DEFAULT_LOCK_INFO = "lockInfo.";
  private DataRecord _dataRecord;

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
  public LockInfo() {
    super(DEFAULT_LOCK_INFO);
    _dataRecord = new DataRecord(DEFAULT_LOCK_INFO);
    setLockInfoFields(DEFAULT_LOCK_ID_TEXT, DEFAULT_OWNER_ID_TEXT, DEFAULT_CLIENT_ID_TEXT, DEFAULT_CLIENT_DATA, DEFAULT_GRANTED_AT_LONG,
        DEFAULT_LAST_RENEWED_AT_LONG, DEFAULT_TIMEOUT_DURATION);
  }

  /**
   * Initialize a LockInfo with a DataRecord, set all info fields to default data
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
      long timeout = dataRecord.getLongField(LockInfoAttribute.TIMEOUT.name(), DEFAULT_TIMEOUT_DURATION);
      setLockInfoFields(lockId,ownerId, clientId, clientData, grantTime,
          lastRenewalTime, timeout);
    }
  }

  /**
   * Initialize a LockInfo with a DataRecord and a Stat, set all info fields to default data
   * @param dataRecord The dataRecord contains lock node data that used to initialize the LockInfo
   * @param stat The stat of the lock node
   */
  public LockInfo(DataRecord dataRecord, MetaClientInterface.Stat stat) {
    this(dataRecord);
    //Synchronize the lockInfo with the stat
    setGrantedAt(stat.getCreationTime());
    setLastRenewedAt(stat.getModifiedTime());
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
                  String clientData, long grantTime, long lastRenewalTime, long timeout) {
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
                                 long timeout) {
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
    _dataRecord.setSimpleField(LockInfoAttribute.LOCK_ID.name(), lockId == null ? DEFAULT_LOCK_ID_TEXT : lockId);
  }

  /**
   * Get the value for OWNER_ID attribute of the lock
   * @param ownerId Represents the initiator of the lock, created by the client.
   *                A service can have multiple ownerId's as long as acquire and release are called
   *                by the same owner.
   */
  public void setOwnerId(String ownerId) {
    _dataRecord.setSimpleField(LockInfoAttribute.OWNER_ID.name(), ownerId == null ? DEFAULT_OWNER_ID_TEXT : ownerId);
  }

  /**
   * Get the value for CLIENT_ID attribute of the lock
   * @param clientId Unique identifier that represents who will get the lock (the client).
   */
  public void setClientId(String clientId) {
    _dataRecord.setSimpleField(LockInfoAttribute.CLIENT_ID.name(), clientId == null ? DEFAULT_CLIENT_ID_TEXT : clientId);
  }

  /**
   * Get the value for CLIENT_DATA attribute of the lock
   * @param clientData String representing the serialized data object
   */
  public void setClientData(String clientData) {
    _dataRecord.setSimpleField(LockInfoAttribute.CLIENT_DATA.name(), clientData == null ? DEFAULT_CLIENT_DATA : clientData);
  }

  /**
   * Get the value for GRANTED_AT attribute of the lock
   * @param grantTime Long representing the time at which the lock was granted
   */
  public void setGrantedAt(Long grantTime) {
    _dataRecord.setLongField(LockInfoAttribute.GRANTED_AT.name(), grantTime);
  }

  /**
   * Get the value for LAST_RENEWED_AT attribute of the lock
   * @param lastRenewalTime Long representing the time at which the lock was last renewed
   */
  public void setLastRenewedAt(Long lastRenewalTime) {
    _dataRecord.setLongField(LockInfoAttribute.LAST_RENEWED_AT.name(), lastRenewalTime);
  }

  /**
   * Get the value for TIMEOUT attribute of the lock
   * @param timeout Long representing the duration of a lock in milliseconds.
   */
  public void setTimeout(long timeout) {
    // Always store the timeout value in milliseconds for the sake of simplicity
    _dataRecord.setLongField(LockInfoAttribute.TIMEOUT.name(), timeout);
  }

  /**
   * Get the value for OWNER_ID attribute of the lock
   * @return the owner id of the lock, {@link #DEFAULT_OWNER_ID_TEXT} if there is no owner id set
   */
  public String getOwnerId() {
    return _dataRecord.getStringField(LockInfoAttribute.OWNER_ID.name(), DEFAULT_OWNER_ID_TEXT);
  }

  /**
   * Get the value for CLIENT_ID attribute of the lock
   * @return the client id of the lock, {@link #DEFAULT_CLIENT_ID_TEXT} if there is no client id set
   */
  public String getClientId() {
    return _dataRecord.getStringField(LockInfoAttribute.CLIENT_ID.name(), DEFAULT_CLIENT_ID_TEXT);
  }

  /**
   * Get the value for LOCK_ID attribute of the lock
   * @return the id of the lock, {@link #DEFAULT_LOCK_ID_TEXT} if there is no lock id set
   */
  public String getLockId() {
    return _dataRecord.getStringField(LockInfoAttribute.LOCK_ID.name(), DEFAULT_LOCK_ID_TEXT);
  }

  /**
   * Get value of CLIENT_DATA
   * @return the string representing the serialized client data, {@link #DEFAULT_CLIENT_DATA}
   * if there is no client data set.
   */
  public String getClientData() {
    return _dataRecord.getStringField(LockInfoAttribute.CLIENT_DATA.name(), DEFAULT_CLIENT_DATA);
  }

  /**
   * Get the time the lock was granted on
   * @return the grant time of the lock, {@link #DEFAULT_GRANTED_AT_LONG}
   * if there is no grant time set
   */
  public Long getGrantedAt() {
    return _dataRecord.getLongField(LockInfoAttribute.GRANTED_AT.name(), DEFAULT_GRANTED_AT_LONG);
  }

  /**
   * Get the last time the lock was renewed
   * @return the last renewal time of the lock, {@link #DEFAULT_LAST_RENEWED_AT_LONG}
   * if there is no renewal time set
   */
  public Long getLastRenewedAt() {
    return _dataRecord.getLongField(LockInfoAttribute.LAST_RENEWED_AT.name(), DEFAULT_LAST_RENEWED_AT_LONG);
  }

  /**
   * Get the value for TIMEOUT attribute of the lock
   * @return the expiring time of the lock, {@link #DEFAULT_TIMEOUT_DURATION} if there is no timeout set
   */
  public long getTimeout() {
    return _dataRecord.getLongField(LockInfoAttribute.TIMEOUT.name(), DEFAULT_TIMEOUT_DURATION);
  }

}

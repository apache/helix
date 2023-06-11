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

import java.util.concurrent.locks.Lock;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Permit extends DataRecord {

  private static final String DEFAULT_PERMIT_INFO = "permitInfo";
  public static final long DEFAULT_TIME_PERMIT_ACQUIRED = -1L;
  public static final long DEFAULT_TIME_SEMAPHORE_CREATED = -1L;
  private boolean _isReleased;

  public enum PermitAttribute {
    TIME_PERMIT_ACQUIRED,
    TIME_SEMAPHORE_CREATED
  }
  public Permit() {
    super(DEFAULT_PERMIT_INFO);
    setPermitFields(DEFAULT_TIME_PERMIT_ACQUIRED, DEFAULT_TIME_SEMAPHORE_CREATED);
  }
  public Permit(DataRecord record) {
    this();
    if (record != null) {
      long timePermitAcquired = record.getLongField(PermitAttribute.TIME_PERMIT_ACQUIRED.name(), DEFAULT_TIME_PERMIT_ACQUIRED);
      long timeSemaphoreCreated = record.getLongField(PermitAttribute.TIME_SEMAPHORE_CREATED.name(), DEFAULT_TIME_SEMAPHORE_CREATED);
      setPermitFields(timePermitAcquired, timeSemaphoreCreated);
    }
  }

  public Permit(DataRecord record, MetaClientInterface.Stat stat) {
    this(record);
    setTimeSemaphoreCreated(stat.getCreationTime());
    setTimePermitAcquired(stat.getModifiedTime());
  }

  public void setPermitFields(long timePermitAcquired, long timeSemaphoreCreated) {
    setTimePermitAcquired(timePermitAcquired);
    setTimeSemaphoreCreated(timeSemaphoreCreated);
    _isReleased = false;
  }

  public void setTimePermitAcquired(long timePermitAcquired) {
    setLongField(PermitAttribute.TIME_PERMIT_ACQUIRED.name(), timePermitAcquired);
  }

  public void setTimeSemaphoreCreated(long timeSemaphoreCreated) {
    setLongField(PermitAttribute.TIME_SEMAPHORE_CREATED.name(), timeSemaphoreCreated);
  }

  public void getTimePermitAcquired() {
    getLongField(PermitAttribute.TIME_PERMIT_ACQUIRED.name(), DEFAULT_TIME_PERMIT_ACQUIRED);
  }

  public void getTimeSemaphoreCreated() {
    getLongField(PermitAttribute.TIME_SEMAPHORE_CREATED.name(), DEFAULT_TIME_SEMAPHORE_CREATED);
  }

  public boolean isReleased() {
    return _isReleased;
  }
  public void releasePermit() {
    _isReleased = true;
  }
}

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ZNRecord;


public class ZKHelixNonblockingLockInfo<T extends String> implements LockInfo<T> {

  private Map<String, String> lockInfo;

  enum InfoKey {
    OWNER, MESSAGE, TIMEOUT
  }

  public ZKHelixNonblockingLockInfo() {
    lockInfo = new HashMap<>();
  }

  @Override
  /**
   * Create a single filed of LockInfo, or update the value of the field if it already exists
   * @param infoKey the key of the LockInfo field
   * @param infoValue the value of the LockInfo field
   */ public void setInfoValue(String infoKey, String infoValue) {
    lockInfo.put(infoKey, infoValue);
  }

  @Override
  /**
   * Get the value of a field in LockInfo
   * @param infoKey the key of the LockInfo field
   * @return the value of the field or null if this key does not exist
   */ public T getInfoValue(String infoKey) {
    return (T) lockInfo.get(infoKey);
  }

  /**
   * Update the lock info with information in a ZNRecord
   * @param record Information about the lock that stored as ZNRecord format
   */
  public void setLockInfoFields(ZNRecord record) {
    if (record == null) {
      return;
    }
    Map<String, String> recordSimpleFields = record.getSimpleFields();
    lockInfo.putAll(recordSimpleFields);
  }
}

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


public class ZKHelixNonblockingLockInfo<K extends ZKHelixNonblockingLockInfo.InfoKey, V extends String> implements LockInfo<K, V> {

  public static final String DEFAULT_OWNER_TEXT = "";
  public static final String DEFAULT_MESSAGE_TEXT = "";
  public static final long DEFAULT_TIMEOUT_LONG = -1L;
  public static final String DEFAULT_TIMEOUT_TEXT = String.valueOf(DEFAULT_TIMEOUT_LONG);
  private Map<InfoKey, String> lockInfo;

  public enum InfoKey {
    OWNER, MESSAGE, TIMEOUT
  }

  /**
   * Constructor of ZKHelixNonblockingLockInfo that set each field to default data
   */
  public ZKHelixNonblockingLockInfo() {
    lockInfo = new HashMap<>();
    lockInfo.put(InfoKey.OWNER, DEFAULT_OWNER_TEXT);
    lockInfo.put(InfoKey.MESSAGE, DEFAULT_MESSAGE_TEXT);
    lockInfo.put(InfoKey.TIMEOUT, DEFAULT_TIMEOUT_TEXT);
  }

  /**
   * Construct a ZKHelixNonblockingLockInfo using a ZNRecord format of data
   * @param znRecord A ZNRecord that contains lock information in its simple fields
   */
  public ZKHelixNonblockingLockInfo(ZNRecord znRecord) {
    this();
    if (znRecord == null) {
      return;
    }
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    lockInfo
        .put(InfoKey.OWNER, simpleFields.getOrDefault(InfoKey.OWNER.name(), DEFAULT_OWNER_TEXT));
    lockInfo.put(InfoKey.MESSAGE,
        simpleFields.getOrDefault(InfoKey.MESSAGE.name(), DEFAULT_MESSAGE_TEXT));
    lockInfo.put(InfoKey.TIMEOUT,
        simpleFields.getOrDefault(InfoKey.TIMEOUT.name(), DEFAULT_TIMEOUT_TEXT));
  }

  @Override
  public void setInfoValue(InfoKey infoKey, String infoValue) {
    lockInfo.put(infoKey, infoValue);
  }

  @Override
  public String getInfoValue(InfoKey infoKey) {
    return lockInfo.get(infoKey);
  }

  /**
   * Method to convert a ZKHelixNonblockingLockInfo to ZNRecord
   * @return a ZNRecord format data contains lock information in its simple fields
   */
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord("");
    znRecord.setSimpleField(InfoKey.OWNER.name(),
        lockInfo.getOrDefault(InfoKey.OWNER, DEFAULT_OWNER_TEXT));
    znRecord.setSimpleField(InfoKey.MESSAGE.name(),
        lockInfo.getOrDefault(InfoKey.MESSAGE, DEFAULT_MESSAGE_TEXT));
    znRecord.setSimpleField(InfoKey.TIMEOUT.name(),
        lockInfo.getOrDefault(InfoKey.TIMEOUT, DEFAULT_TIMEOUT_TEXT));
    return znRecord;
  }
}

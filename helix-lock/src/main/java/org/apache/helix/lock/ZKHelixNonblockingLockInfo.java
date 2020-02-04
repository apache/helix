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
  public static final String DEFAULT_TIMEOUT_TEXT = String.valueOf(-1);
  private Map<InfoKey, String> lockInfo;

  public enum InfoKey {
    OWNER, MESSAGE, TIMEOUT
  }

  public ZKHelixNonblockingLockInfo() {
    lockInfo = new HashMap<>();
    lockInfo.put(InfoKey.OWNER, DEFAULT_OWNER_TEXT);
    lockInfo.put(InfoKey.MESSAGE, DEFAULT_MESSAGE_TEXT);
    lockInfo.put(InfoKey.TIMEOUT, DEFAULT_TIMEOUT_TEXT);
  }

  public ZKHelixNonblockingLockInfo(ZNRecord znRecord) {
    this();
    lockInfo.put(InfoKey.OWNER, znRecord.getSimpleField(InfoKey.OWNER.name()));
    lockInfo.put(InfoKey.MESSAGE, znRecord.getSimpleField(InfoKey.MESSAGE.name()));
    lockInfo.put(InfoKey.TIMEOUT, znRecord.getSimpleField(InfoKey.TIMEOUT.name()));
  }

  @Override
  public void setInfoValue(InfoKey infoKey, String infoValue) {
    lockInfo.put(infoKey, infoValue);
  }

  @Override
  public String getInfoValue(InfoKey infoKey) {
    return lockInfo.get(infoKey);
  }
}

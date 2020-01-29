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

  private Map<String, String>  lockInfo;

  public ZKHelixNonblockingLockInfo() {
    lockInfo = new HashMap<>();
  }

  @Override
  public void setInfoValue(String infoKey, String infoValue) {
    lockInfo.put(infoKey, infoValue);
  }

  @Override
  public T getInfoValue(String infoKey) {
    return (T)lockInfo.get(infoKey);
  }

  public void setLockInfoFields(ZNRecord record) {
    Map<String, String> recordSimpleFields = record.getSimpleFields();
    lockInfo.putAll(recordSimpleFields);
  }
}

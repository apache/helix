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

import org.apache.helix.metaclient.datamodel.DataRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LockInfoTest {
  private static final String OWNER_ID = "urn:li:principal:UNKNOWN";
  private static final String CLIENT_ID = "test_client_id";
  private static final String CLIENT_DATA = "client_data";
  private static final String LOCK_ID = "794c8a4c-c14b-4c23-b83f-4e1147fc6978";
  private static final long GRANT_TIME = System.currentTimeMillis();
  private static final long LAST_RENEWAL_TIME = System.currentTimeMillis();
  private static final Duration TIMEOUT = Duration.ofMillis(100000);

  public static final String DEFAULT_LOCK_ID_TEXT = "";
  public static final String DEFAULT_OWNER_ID_TEXT = "";
  public static final String DEFAULT_CLIENT_ID_TEXT = "";
  public static final String DEFAULT_CLIENT_DATA = "";
  public static final long DEFAULT_GRANTED_AT_LONG = -1L;
  public static final long DEFAULT_LAST_RENEWED_AT_LONG = -1L;
  public static final Duration DEFAULT_TIMEOUT_DURATION = Duration.ofMillis(-1L);

  @Test
  public void testLockInfo() {
    LockInfo lockInfo =
        new LockInfo(LOCK_ID, OWNER_ID, CLIENT_ID, CLIENT_DATA, GRANT_TIME,
            LAST_RENEWAL_TIME, TIMEOUT);

    Assert.assertEquals(LOCK_ID, lockInfo.getLockId());
    Assert.assertEquals(OWNER_ID, lockInfo.getOwnerId());
    Assert.assertEquals(CLIENT_ID, lockInfo.getClientId());
    Assert.assertEquals(CLIENT_DATA, lockInfo.getClientData());
    Assert.assertEquals(GRANT_TIME, (long) lockInfo.getGrantedAt());
    Assert.assertEquals(LAST_RENEWAL_TIME, (long) lockInfo.getLastRenewedAt());
    Assert.assertEquals(TIMEOUT, lockInfo.getTimeout());

    DataRecord dataRecord = new DataRecord("dataRecord");
    LockInfo lockInfo1 = new LockInfo(dataRecord);
    Assert.assertEquals(DEFAULT_LOCK_ID_TEXT, lockInfo1.getLockId());
    Assert.assertEquals(DEFAULT_OWNER_ID_TEXT, lockInfo1.getOwnerId());
    Assert.assertEquals(DEFAULT_CLIENT_ID_TEXT, lockInfo1.getClientId());
    Assert.assertEquals(DEFAULT_CLIENT_DATA, lockInfo1.getClientData());
    Assert.assertEquals(DEFAULT_GRANTED_AT_LONG, (long) lockInfo1.getGrantedAt());
    Assert.assertEquals(DEFAULT_LAST_RENEWED_AT_LONG, (long) lockInfo1.getLastRenewedAt());
    Assert.assertEquals(DEFAULT_TIMEOUT_DURATION, lockInfo1.getTimeout());
  }
}

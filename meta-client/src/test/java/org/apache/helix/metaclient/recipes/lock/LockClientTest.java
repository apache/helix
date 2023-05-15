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

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LockClientTest extends ZkMetaClientTestBase {

  private static final String TEST_INVALID_PATH = "/_invalid/a/b/c";

  public LockClient createLockClient() {

    MetaClientConfig.StoreType storeType = MetaClientConfig.StoreType.ZOOKEEPER;
    MetaClientConfig config = new MetaClientConfig.MetaClientConfigBuilder<>().setConnectionAddress(ZK_ADDR)
        .setStoreType(storeType).build();
    return new LockClient(config);
  }

  @Test
  public void testAcquireLock() {
    final String key = "/TestLockClient_testAcquireLock";
    LockClient lockClient = createLockClient();
    LockInfo lockInfo = new LockInfo();
    lockClient.acquireLock(key, lockInfo, MetaClientInterface.EntryMode.PERSISTENT);
    Assert.assertNotNull(lockClient.retrieveLock(key));
    try {
      lockClient.acquireLock(TEST_INVALID_PATH, new LockInfo(), MetaClientInterface.EntryMode.PERSISTENT);
      Assert.fail("Should not be able to acquire lock for key: " + key);
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testReleaseLock() {
    final String key = "/TestLockClient_testReleaseLock";
    LockClient lockClient = createLockClient();
    LockInfo lockInfo = new LockInfo();
    lockClient.acquireLock(key, lockInfo, MetaClientInterface.EntryMode.PERSISTENT);
    Assert.assertNotNull(lockClient.retrieveLock(key));

    lockClient.releaseLock(key);
    Assert.assertNull(lockClient.retrieveLock(key));
    lockClient.releaseLock(TEST_INVALID_PATH);
  }

  @Test
  public void testAcquireTTLLock() {
    final String key = "/TestLockClient_testAcquireTTLLock";
    LockClient lockClient = createLockClient();
    LockInfo lockInfo = new LockInfo();
    lockClient.acquireLockWithTTL(key, lockInfo, 1L);
    Assert.assertNotNull(lockClient.retrieveLock(key));
    try {
      lockClient.acquireLockWithTTL(TEST_INVALID_PATH, lockInfo, 1L);
      Assert.fail("Should not be able to acquire lock for key: " + key);
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testRetrieveLock() {
    final String key = "/TestLockClient_testRetrieveLock";
    LockClient lockClient = createLockClient();
    LockInfo lockInfo = new LockInfo();
    lockClient.acquireLock(key, lockInfo, MetaClientInterface.EntryMode.PERSISTENT);
    Assert.assertNotNull(lockClient.retrieveLock(key));
    Assert.assertNull(lockClient.retrieveLock(TEST_INVALID_PATH));
  }

  @Test
  public void testRenewTTLLock() {
    final String key = "/TestLockClient_testRenewTTLLock";
    LockClient lockClient = createLockClient();
    LockInfo lockInfo = new LockInfo();
    lockClient.acquireLockWithTTL(key, lockInfo, 1L);
    Assert.assertNotNull(lockClient.retrieveLock(key));

    lockClient.renewTTLLock(key);
    Assert.assertNotSame(lockClient.retrieveLock(key).getGrantedAt(), lockInfo.getLastRenewedAt());
    try {
      lockClient.renewTTLLock(TEST_INVALID_PATH);
      Assert.fail("Should not be able to renew lock for key: " + key);
    } catch (Exception e) {
      // expected
    }
  }
}

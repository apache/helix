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

import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;

public class DistributedSemaphoreTest extends ZkMetaClientTestBase {

  public DistributedSemaphore createSemaphoreClientAndSemaphore(String path, int capacity) {

    MetaClientConfig.StoreType storeType = MetaClientConfig.StoreType.ZOOKEEPER;
    MetaClientConfig config = new MetaClientConfig.MetaClientConfigBuilder<>().setConnectionAddress(ZK_ADDR)
        .setStoreType(storeType).build();
    DistributedSemaphore client = new DistributedSemaphore(config);
    client.createSemaphore(path, capacity);
    return client;
  }

  @Test
  public void testAcquirePermit() {
    final String key = "/TestSemaphore_testAcquirePermit";
    int capacity = 5;
    DistributedSemaphore semaphoreClient = createSemaphoreClientAndSemaphore(key, capacity);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity);

    Permit permit = semaphoreClient.acquire();
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity - 1);
  }

  @Test
  public void testAcquireMultiplePermits() {
    final String key = "/TestSemaphore_testAcquireMultiplePermits";
    int capacity = 5;
    int count = 4;
    DistributedSemaphore semaphoreClient = createSemaphoreClientAndSemaphore(key, capacity);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity);

    Collection<Permit> permits = semaphoreClient.acquire(count);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity - 4);
    Assert.assertEquals(permits.size(), count);
    Assert.assertNull(semaphoreClient.acquire(count));
  }

  @Test
  public void testReturnPermit() {
    final String key = "/TestSemaphore_testReturnPermit";
    int capacity = 5;
    DistributedSemaphore semaphoreClient = createSemaphoreClientAndSemaphore(key, capacity);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity);

    Permit permit = semaphoreClient.acquire();
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity - 1);

    semaphoreClient.returnPermit(permit);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity);

    // return the same permit again. Should not fail but capacity remains same.
    semaphoreClient.returnPermit(permit);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity);
  }

  @Test
  public void testReturnMultiplePermits() {
    final String key = "/TestSemaphore_testReturnMultiplePermits";
    int capacity = 5;
    int count = 4;
    DistributedSemaphore semaphoreClient = createSemaphoreClientAndSemaphore(key, capacity);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity);

    Collection<Permit> permits = semaphoreClient.acquire(count);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity - 4);
    Assert.assertEquals(permits.size(), count);

    semaphoreClient.returnAllPermits(permits);
    Assert.assertEquals(semaphoreClient.getRemainingCapacity(), capacity);
  }

  @Test
  public void testTryAcquirePermit() {
    // Not implemented
  }

}

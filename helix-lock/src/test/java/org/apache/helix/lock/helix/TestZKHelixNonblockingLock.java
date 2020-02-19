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

package org.apache.helix.lock.helix;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.lock.LockInfo;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZKHelixNonblockingLock extends ZkTestBase {

  private final String _clusterName = TestHelper.getTestClassName();
  private final String _lockMessage = "Test";
  private String _lockPath;
  private ZKDistributedNonblockingLock _lock;
  private String _userId;
  private HelixLockScope _participantScope;

  @BeforeClass
  public void beforeClass() throws Exception {

    System.out.println("START " + _clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 10, 5, 3,
        "MasterSlave", true);
    _userId = UUID.randomUUID().toString();

    List<String> pathKeys = new ArrayList<>();
    pathKeys.add(_clusterName);
    pathKeys.add(_clusterName);

    _participantScope = new HelixLockScope(HelixLockScope.LockScopeProperty.CLUSTER, pathKeys);
    _lockPath = _participantScope.getPath();
    _lock = new ZKDistributedNonblockingLock(_participantScope, ZK_ADDR, Long.MAX_VALUE, _lockMessage,
        _userId);
  }

  @BeforeMethod
  public void beforeMethod() {
    _gZkClient.delete(_lockPath);
    Assert.assertFalse(_gZkClient.exists(_lockPath));
  }

  @Test
  public void testAcquireLock() {

    // Acquire lock
    _lock.acquireLock();
    Assert.assertTrue(_gZkClient.exists(_lockPath));

    // Get lock information
    LockInfo lockInfo = _lock.getCurrentLockInfo();
    Assert.assertEquals(lockInfo.getOwner(), _userId);
    Assert.assertEquals(lockInfo.getMessage(), _lockMessage);

    // Check if the user is lock owner
    Assert.assertTrue(_lock.isCurrentOwner());

    // Release lock
    _lock.releaseLock();
    Assert.assertFalse(_lock.isCurrentOwner());
  }

  @Test
  public void testAcquireLockWhenExistingLockNotExpired() {

    // Fake condition when the lock owner is not current user
    String fakeUserID = UUID.randomUUID().toString();
    ZNRecord fakeRecord = new ZNRecord(fakeUserID);
    fakeRecord.setSimpleField(LockInfo.LockInfoAttribute.OWNER.name(), fakeUserID);
    fakeRecord
        .setSimpleField(LockInfo.LockInfoAttribute.TIMEOUT.name(), String.valueOf(Long.MAX_VALUE));
    _gZkClient.create(_lockPath, fakeRecord, CreateMode.PERSISTENT);

    // Check if the user is lock owner
    Assert.assertFalse(_lock.isCurrentOwner());

    // Acquire lock
    Assert.assertFalse(_lock.acquireLock());
    Assert.assertFalse(_lock.isCurrentOwner());

    // Release lock
    Assert.assertFalse(_lock.releaseLock());
  }

  @Test
  public void testAcquireLockWhenExistingLockExpired() {

    // Fake condition when the current lock already expired
    String fakeUserID = UUID.randomUUID().toString();
    ZNRecord fakeRecord = new ZNRecord(fakeUserID);
    fakeRecord.setSimpleField(LockInfo.LockInfoAttribute.OWNER.name(), fakeUserID);
    fakeRecord.setSimpleField(LockInfo.LockInfoAttribute.TIMEOUT.name(),
        String.valueOf(System.currentTimeMillis()));
    _gZkClient.create(_lockPath, fakeRecord, CreateMode.PERSISTENT);

    // Acquire lock
    Assert.assertTrue(_lock.acquireLock());
    Assert.assertTrue(_lock.isCurrentOwner());

    // Release lock
    Assert.assertTrue(_lock.releaseLock());
    Assert.assertFalse(_lock.isCurrentOwner());
  }

  @Test
  public void testSimultaneousAcquire() {
    List<Callable<Boolean>> threads = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      ZKDistributedNonblockingLock lock =
          new ZKDistributedNonblockingLock(_participantScope, ZK_ADDR, Long.MAX_VALUE, _lockMessage,
              UUID.randomUUID().toString());
      threads.add(new TestSimultaneousAcquireLock(lock));
    }
    Map<String, Boolean> resultMap = TestHelper.startThreadsConcurrently(threads, 1000);
    Assert.assertEquals(resultMap.size(), 2);
    Assert.assertEqualsNoOrder(resultMap.values().toArray(), new Boolean[]{true, false});
  }

  private static class TestSimultaneousAcquireLock implements Callable<Boolean> {
    final ZKDistributedNonblockingLock _lock;

    TestSimultaneousAcquireLock(ZKDistributedNonblockingLock lock) {
      _lock = lock;
    }

    @Override
    public Boolean call() throws Exception {
      return _lock.acquireLock();
    }
  }
}


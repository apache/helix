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

import java.util.Date;
import java.util.UUID;

import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestZKHelixNonblockingLock extends ZkUnitTestBase {

  private final String _lockPath = "/testLockPath";
  private final String _className = TestHelper.getTestClassName();
  private final String _methodName = TestHelper.getTestMethodName();
  private final String _clusterName = _className + "_" + _methodName;
  private final String _lockMessage = "Test";

  @BeforeClass
  public void beforeClass() throws Exception {

    System.out.println("START " + _clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 10, 5, 3,
        "MasterSlave", true);
  }

  @Test
  public void testAcquireLockWithParticipantScope() {

    // Initialize lock with participant config scope
    HelixConfigScope participantScope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT).forCluster(_clusterName)
            .forParticipant("localhost_12918").build();

    String userID = UUID.randomUUID().toString();

    ZKHelixNonblockingLock lock =
        new ZKHelixNonblockingLock(_clusterName, participantScope, ZK_ADDR, Long.MAX_VALUE,
            _lockMessage, userID);

    // Acquire lock
    lock.acquireLock();
    String lockPath = "/" + _clusterName + '/' + "LOCKS" + '/' + participantScope;
    Assert.assertTrue(_gZkClient.exists(lockPath));

    // Get lock information
    LockInfo<String> record = lock.getLockInfo();
    Assert
        .assertEquals(record.getInfoValue(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name()), userID);
    Assert.assertEquals(record.getInfoValue(ZKHelixNonblockingLockInfo.InfoKey.MESSAGE.name()),
        _lockMessage);

    // Check if the user is lock owner
    Assert.assertTrue(lock.isOwner());

    // Release lock
    lock.releaseLock();
    Assert.assertFalse(_gZkClient.exists(lockPath));
  }

  @Test
  public void testAcquireLockWithUserProvidedPath() {

    // Initialize lock with user provided path
    String userID = UUID.randomUUID().toString();

    ZKHelixNonblockingLock lock =
        new ZKHelixNonblockingLock(_lockPath, ZK_ADDR, Long.MAX_VALUE, _lockMessage, userID);

    //Acquire lock
    lock.acquireLock();
    Assert.assertTrue(_gZkClient.exists(_lockPath));

    // Get lock information
    LockInfo<String> record = lock.getLockInfo();
    Assert
        .assertEquals(record.getInfoValue(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name()), userID);
    Assert.assertEquals(record.getInfoValue(ZKHelixNonblockingLockInfo.InfoKey.MESSAGE.name()),
        _lockMessage);

    // Check if the user is lock owner
    Assert.assertTrue(lock.isOwner());

    // Release lock
    lock.releaseLock();
    Assert.assertFalse(_gZkClient.exists(_lockPath));
  }

  @Test
  public void testAcquireLockWhenExistingLockNotExpired() {

    // Initialize lock with user provided path
    String ownerID = UUID.randomUUID().toString();

    ZKHelixNonblockingLock lock =
        new ZKHelixNonblockingLock(_lockPath, ZK_ADDR, 0L, _lockMessage, ownerID);

    // Fake condition when the lock owner is not current user
    String fakeUserID = UUID.randomUUID().toString();
    ZNRecord fakeRecord = new ZNRecord(fakeUserID);
    fakeRecord.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name(), fakeUserID);
    fakeRecord.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.TIMEOUT.name(),
        String.valueOf(Long.MAX_VALUE));
    _gZkClient.create(_lockPath, fakeRecord, CreateMode.PERSISTENT);

    // Check if the user is lock owner
    Assert.assertFalse(lock.isOwner());

    // Acquire lock
    Assert.assertFalse(lock.acquireLock());
    Assert.assertFalse(lock.isOwner());

    // Release lock
    Assert.assertFalse(lock.releaseLock());
    Assert.assertTrue(_gZkClient.exists(_lockPath));

    _gZkClient.delete(_lockPath);
  }

  @Test
  public void testAcquireLockWhenExistingLockExpired() {

    // Initialize lock with user provided path
    String ownerID = UUID.randomUUID().toString();

    ZKHelixNonblockingLock lock =
        new ZKHelixNonblockingLock(_lockPath, ZK_ADDR, Long.MAX_VALUE, _lockMessage, ownerID);

    // Fake condition when the current lock already expired
    String fakeUserID = UUID.randomUUID().toString();
    ZNRecord fakeRecord = new ZNRecord(fakeUserID);
    fakeRecord.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.OWNER.name(), fakeUserID);
    fakeRecord.setSimpleField(ZKHelixNonblockingLockInfo.InfoKey.TIMEOUT.name(),
        String.valueOf(System.currentTimeMillis()));
    _gZkClient.create(_lockPath, fakeRecord, CreateMode.PERSISTENT);

    // Acquire lock
    Assert.assertTrue(lock.acquireLock());
    Assert.assertTrue(_gZkClient.exists(_lockPath));

    // Check if the current user is the lock owner
    Assert.assertTrue(lock.isOwner());

    // Release lock
    Assert.assertTrue(lock.releaseLock());
    Assert.assertFalse(_gZkClient.exists(_lockPath));
  }
}


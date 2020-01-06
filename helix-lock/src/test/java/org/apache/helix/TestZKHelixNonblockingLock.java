package org.apache.helix;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Date;

import org.apache.helix.lock.zk.ZKHelixUnblockingLock;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZKHelixNonblockingLock extends ZkUnitTestBase {

  public final String LOCK_PATH = "/testLockPath";

  @Test
  public void testBasicLock() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 10, 5, 3,
        "MasterSlave", true);

    // participant scope config
    HelixConfigScope participantScope = new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT)
        .forCluster(clusterName).forParticipant("localhost_12918").build();

    ZKHelixUnblockingLock basicLock =
        new ZKHelixUnblockingLock(clusterName, participantScope, ZK_ADDR, null, null);
    basicLock.acquireLock();
    String lockPath = "/" + clusterName + '/' + "LOCKS" + '/' + participantScope;

    Assert.assertTrue(_gZkClient.exists(lockPath));

    ZNRecord record = basicLock.getLockInfo();
    Assert.assertEquals(record, null);
    basicLock.releaseLock();
    Assert.assertFalse(_gZkClient.exists(lockPath));

    ZKHelixUnblockingLock basicLockWithPath =
        new ZKHelixUnblockingLock(LOCK_PATH, ZK_ADDR, null, null);
    basicLockWithPath.acquireLock();

    Assert.assertTrue(_gZkClient.exists(LOCK_PATH));
    record = basicLockWithPath.getLockInfo();
    Assert.assertEquals(record, null);
    basicLockWithPath.releaseLock();
    Assert.assertFalse(_gZkClient.exists(LOCK_PATH));
  }
}

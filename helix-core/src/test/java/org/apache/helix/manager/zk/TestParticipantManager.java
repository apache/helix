package org.apache.helix.manager.zk;

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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import org.apache.helix.PreConnectCallback;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestParticipantManager extends ZkTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestParticipantManager.class);

  /*
   * Simulates zk session expiry before creating live instance in participant manager. This test
   * makes sure the session aware create ephemeral API is called, which validates the expected zk
   * session.
   * What this test does is:
   * 1. Sets up live instance with session S0
   * 2. Expires S0 and gets new session S1
   * 3. S1 is blocked before creating live instance in participant manager
   * 4. Expires S1 and gets new session S2
   * 5. Proceeds S1 to create live instance, which will fail because session S1 is expired
   * 6. Proceeds S2 to create live instance, which will succeed
   */
  @Test
  public void testSessionExpiryCreateLiveInstance() throws Exception {
    final String className = TestHelper.getTestClassName();
    final String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    final ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(ZK_ADDR));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    TestHelper.setupCluster(clusterName, ZK_ADDR,
        12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave",
        true); // do rebalance

    final String instanceName = "localhost_12918";
    final MockParticipantManager manager =
        new MockParticipantManager(ZK_ADDR, clusterName, instanceName);

    manager.syncStart();

    final LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    final long originalCreationTime = liveInstance.getStat().getCreationTime();
    final String originalSessionId = manager.getSessionId();

    // Verify current live instance.
    Assert.assertNotNull(liveInstance);
    Assert.assertEquals(liveInstance.getEphemeralOwner(), originalSessionId);

    final CountDownLatch startCountdown = new CountDownLatch(1);
    final CountDownLatch endCountdown = new CountDownLatch(1);
    final Semaphore semaphore = new Semaphore(0);
    manager.addPreConnectCallback(
        new BlockingPreConnectCallback(instanceName, startCountdown, endCountdown, semaphore));

    // Expire S0 and new session S1 will be created.
    ZkTestHelper.asyncExpireSession(manager.getZkClient());

    // Wait for onPreConnect to start
    semaphore.acquire();

    // New session S1 should not be equal to S0.
    Assert.assertFalse(originalSessionId.equals(manager.getSessionId()));

    // Live instance should be gone as original session S0 is expired.
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance(instanceName)));

    final String sessionOne = manager.getSessionId();

    // Expire S1 when S1 is blocked in onPreConnect().
    // New session S2 will be created.
    ZkTestHelper.asyncExpireSession(manager.getZkClient());

    TestHelper.verify(
        () -> !(ZKUtil.toHexSessionId(manager.getZkClient().getSessionId()).equals(sessionOne)),
        TestHelper.WAIT_DURATION);

    // New session S2 should not be equal to S1.
    final String sessionTwo = ZKUtil.toHexSessionId(manager.getZkClient().getSessionId());
    Assert.assertFalse(sessionOne.equals(sessionTwo));

    // Proceed S1 to create live instance, which will fail.
    startCountdown.countDown();

    // Wait until S2 starts onPreConnect, which indicates S1's handling new session is completed.
    semaphore.acquire();

    // Live instance should not be created because zk session is expired.
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance(instanceName)),
        "Live instance should not be created because zk session is expired!");

    // Proceed S2 to create live instance.
    endCountdown.countDown();

    TestHelper.verify(() -> {
      // Newly created live instance should be created by the latest session S2
      // and have a new creation time.
      LiveInstance newLiveInstance =
          accessor.getProperty(keyBuilder.liveInstance(instanceName));
      return newLiveInstance != null
          && newLiveInstance.getStat().getCreationTime() != originalCreationTime
          && newLiveInstance.getEphemeralOwner().equals(sessionTwo);
    }, TestHelper.WAIT_DURATION);

    // Clean up.
    manager.syncStop();
    deleteCluster(clusterName);
  }

  @Test(dependsOnMethods = "testSessionExpiryCreateLiveInstance")
  public void testCurrentTaskThreadPoolSizeCreation() throws Exception {
    // Using a pool sized different from the default value to verify correctness
    final int testThreadPoolSize = TaskConstants.DEFAULT_TASK_THREAD_POOL_SIZE + 1;

    final String className = TestHelper.getTestClassName();
    final String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;

    final ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName,
        new ZkBaseDataAccessor.Builder<ZNRecord>().setZkAddress(ZK_ADDR).build());
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    final String instanceName = "localhost_12918";
    final MockParticipantManager manager =
        new MockParticipantManager(ZK_ADDR, clusterName, instanceName);

    InstanceConfig instanceConfig = accessor.getProperty(keyBuilder.instanceConfig(instanceName));
    instanceConfig.setTargetTaskThreadPoolSize(testThreadPoolSize);
    accessor.setProperty(keyBuilder.instanceConfig(instanceName), instanceConfig);

    manager.syncStart();

    final LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    Assert.assertNotNull(liveInstance);
    Assert.assertEquals(liveInstance.getCurrentTaskThreadPoolSize(), testThreadPoolSize);

    // Clean up.
    manager.syncStop();
    deleteCluster(clusterName);
  }

  /*
   * Mocks PreConnectCallback to insert session expiry during ParticipantManager#handleNewSession()
   */
  static class BlockingPreConnectCallback implements PreConnectCallback {
    private final String instanceName;
    private final CountDownLatch startCountDown;
    private final CountDownLatch endCountDown;
    private final Semaphore semaphore;

    private boolean canCreateLiveInstance;

    BlockingPreConnectCallback(String instanceName, CountDownLatch startCountdown,
        CountDownLatch endCountdown, Semaphore semaphore) {
      this.instanceName = instanceName;
      this.startCountDown = startCountdown;
      this.endCountDown = endCountdown;
      this.semaphore = semaphore;
    }

    @Override
    public void onPreConnect() {
      LOG.info("Handling new session for instance: {}", instanceName);
      semaphore.release();
      try {
        LOG.info("Waiting session expiry to happen.");
        startCountDown.await();
        if (canCreateLiveInstance) {
          LOG.info("Waiting to continue creating live instance.");
          endCountDown.await();
        }
      } catch (InterruptedException ex) {
        LOG.error("Interrupted in waiting", ex);
      }
      canCreateLiveInstance = true;
    }
  }
}

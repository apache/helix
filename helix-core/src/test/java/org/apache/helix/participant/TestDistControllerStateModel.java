package org.apache.helix.participant;

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

import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDistControllerStateModel extends ZkUnitTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestDistControllerStateModel.class);

  final String clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  DistClusterControllerStateModel stateModel = null;

  @BeforeMethod()
  public void beforeMethod() {
    stateModel = new DistClusterControllerStateModel(ZK_ADDR);
    if (_gZkClient.exists("/" + clusterName)) {
      _gZkClient.deleteRecursively("/" + clusterName);
    }
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    if (_gZkClient.exists("/" + clusterName)) {
      TestHelper.dropCluster(clusterName, _gZkClient);
    }
  }

  @Test()
  public void testOnBecomeStandbyFromOffline() {
    stateModel.onBecomeStandbyFromOffline(new Message(new ZNRecord("test")), null);
  }

  @Test()
  public void testOnBecomeLeaderFromStandby() {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");
    try {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    } catch (Exception e) {
      LOG.error("Exception becoming leader from standby", e);
    }
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }

  @Test()
  public void testOnBecomeStandbyFromLeader() {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");
    stateModel.onBecomeStandbyFromLeader(message, new NotificationContext(null));
  }

  @Test()
  public void testOnBecomeOfflineFromStandby() {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");

    stateModel.onBecomeOfflineFromStandby(message, null);
  }

  @Test()
  public void testOnBecomeDroppedFromOffline() {
    stateModel.onBecomeDroppedFromOffline(null, null);
  }

  @Test()
  public void testOnBecomeOfflineFromDropped() {
    stateModel.onBecomeOfflineFromDropped(null, null);
  }

  @Test()
  public void testRollbackOnError() {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");
    try {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    } catch (Exception e) {
      LOG.error("Exception becoming leader from standby", e);
    }
    stateModel.rollbackOnError(message, new NotificationContext(null), null);
  }

  @Test()
  public void testReset() {
    Message message = new Message(MessageType.STATE_TRANSITION, "0");
    message.setPartitionName(clusterName);
    message.setTgtName("controller_0");
    try {
      stateModel.onBecomeLeaderFromStandby(message, new NotificationContext(null));
    } catch (Exception e) {
      LOG.error("Exception becoming leader from standby", e);
    }
    stateModel.reset();
  }

  /**
   * Test to verify that different DistClusterControllerStateModel instances
   * use separate lock objects, ensuring no cross-instance blocking.
   */
  @Test()
  public void testNoSharedLockAcrossInstances() throws Exception {
    LOG.info("Testing that lock objects are not shared across DistClusterControllerStateModel instances");

    // Verify different instances have different lock objects
    DistClusterControllerStateModel instance1 = new DistClusterControllerStateModel(ZK_ADDR);
    DistClusterControllerStateModel instance2 = new DistClusterControllerStateModel(ZK_ADDR);

    Field lockField = DistClusterControllerStateModel.class.getDeclaredField("_controllerLock");
    lockField.setAccessible(true);

    Object lock1 = lockField.get(instance1);
    Object lock2 = lockField.get(instance2);

    Assert.assertNotNull(lock1, "First instance should have a lock object");
    Assert.assertNotNull(lock2, "Second instance should have a lock object");
    Assert.assertNotSame(lock1, lock2, "Different instances must have different lock objects");

    // Verify concurrent access doesn't block across instances
    final int NUM_INSTANCES = 10;
    ExecutorService executor = Executors.newFixedThreadPool(NUM_INSTANCES);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(NUM_INSTANCES);
    AtomicInteger completedInstances = new AtomicInteger(0);

    for (int i = 0; i < NUM_INSTANCES; i++) {
      final int instanceId = i;
      final DistClusterControllerStateModel instance = new DistClusterControllerStateModel(ZK_ADDR);

      executor.submit(() -> {
        try {
          startLatch.await(); // wait for all threads to be ready

          // Simulate state transition operations that would use the lock
          synchronized (lockField.get(instance)) {
            // hold the lock here briefly to simulate real state transition work
            Thread.sleep(100);
            completedInstances.incrementAndGet();
          }

        } catch (Exception e) {
          LOG.error("Instance {} failed during concurrent test", instanceId, e);
        } finally {
          completionLatch.countDown();
        }
      });
    }

    // start all threads simultaneously
    startLatch.countDown();

    // All instances should complete within reasonable time since they don't block each other
    boolean allCompleted = completionLatch.await(500, TimeUnit.MILLISECONDS);

    executor.shutdown();
    executor.awaitTermination(2, TimeUnit.SECONDS);

    Assert.assertTrue(allCompleted, "All instances should complete without blocking each other");
    Assert.assertEquals(completedInstances.get(), NUM_INSTANCES,
        "All instances should successfully complete their synchronized work");
  }

  /**
   * Explicit test to verify that while one instance holds its lock indefinitely,
   * another instance with a different lock can complete immediately.
   */
  @Test()
  public void testExplicitLockIndependence() throws Exception {
    LOG.info("Testing explicit lock independence - one blocked, other should complete");

    DistClusterControllerStateModel instance1 = new DistClusterControllerStateModel(ZK_ADDR);
    DistClusterControllerStateModel instance2 = new DistClusterControllerStateModel(ZK_ADDR);

    Field lockField = DistClusterControllerStateModel.class.getDeclaredField("_controllerLock");
    lockField.setAccessible(true);

    Object lock1 = lockField.get(instance1);
    Object lock2 = lockField.get(instance2);

    Assert.assertNotSame(lock1, lock2, "Different instances must have different lock objects");

    CountDownLatch instance1Started = new CountDownLatch(1);
    CountDownLatch instance2Completed = new CountDownLatch(1);
    AtomicBoolean instance1Interrupted = new AtomicBoolean(false);

    // Thread 1: Hold lock1 for 5 seconds
    Thread thread1 = new Thread(() -> {
      try {
        synchronized (lock1) {
          instance1Started.countDown();
          Thread.sleep(5000); // Hold much longer than test timeout
        }
      } catch (InterruptedException e) {
        instance1Interrupted.set(true);
        Thread.currentThread().interrupt();
      }
    }, "BlockingThread");

    // Thread 2: Should complete immediately since it uses lock2
    Thread thread2 = new Thread(() -> {
      try {
        instance1Started.await(1000, TimeUnit.MILLISECONDS); // Wait for thread1 to acquire lock1
        synchronized (lock2) {
          // Should acquire immediately since lock2 != lock1
          Thread.sleep(50);
          instance2Completed.countDown();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }, "NonBlockingThread");

    thread1.start();
    thread2.start();

    // Instance2 should complete immediately even though instance1 is blocked
    boolean instance2CompletedQuickly = instance2Completed.await(200, TimeUnit.MILLISECONDS);

    // Clean up
    thread1.interrupt();
    thread1.join(1000);
    thread2.join(1000);

    Assert.assertTrue(instance2CompletedQuickly,
        "Instance2 should complete immediately, proving locks are not shared");
    Assert.assertTrue(instance1Interrupted.get(),
        "Instance1 should have been interrupted while holding its lock");
  }
}
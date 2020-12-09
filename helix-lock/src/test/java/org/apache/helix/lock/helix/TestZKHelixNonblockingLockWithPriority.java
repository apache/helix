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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.HelixException;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestZKHelixNonblockingLockWithPriority extends ZkTestBase {

  private final String _clusterName = TestHelper.getTestClassName();
  private String _lockPath;
  private HelixLockScope _participantScope;
  private AtomicBoolean _isCleanupNotified;

  private final LockListener _lockListener = new LockListener() {
    @Override
    public void onCleanupNotification() {
      _isCleanupNotified.set(true);
      try {
        Thread.sleep(20000);
      } catch (Exception e) {

      }
    }
  };

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + _clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, 10, 5, 3,
        "MasterSlave", true);

    List<String> pathKeys = new ArrayList<>();
    pathKeys.add(_clusterName);
    pathKeys.add(_clusterName);
    _participantScope = new HelixLockScope(HelixLockScope.LockScopeProperty.CLUSTER, pathKeys);
    _lockPath = _participantScope.getPath();

    _isCleanupNotified = new AtomicBoolean(false);
  }

  @BeforeMethod
  public void beforeMethod() {
    _gZkClient.delete(_lockPath);
    Assert.assertFalse(_gZkClient.exists(_lockPath));
  }

  @AfterSuite
  public void afterSuite() throws IOException {
    super.afterSuite();
  }

  @Test
  public void testLowerPriorityRequestRejected() throws Exception {
    ZKDistributedNonblockingLock lock = createLockWithConfig();
    ZKDistributedNonblockingLock.Builder lockBuilder = new ZKDistributedNonblockingLock.Builder();
    lockBuilder.setLockScope(_participantScope).setZkAddress(ZK_ADDR).setTimeout(3600000L)
        .setLockMsg("lower priority lock").setUserId("low_lock").setPriority(0).setWaitingTimeout(1000)
        .setCleanupTimeout(2000).setIsForceful(false).setLockListener(createLockListener());
    ZKDistributedNonblockingLock lowerLock = lockBuilder.build();

    Thread t = new Thread() {
      @Override
      public void run() {
        lock.tryLock();
      }
    };

    t.start();
    t.join();
    Assert.assertTrue(lock.isCurrentOwner());

    LockHelper lockHelper = new LockHelper(lowerLock);
    Thread threadLow = new Thread(lockHelper);
    threadLow.start();
    threadLow.join();
    boolean lowResult = lockHelper.getResult();

    Assert.assertFalse(_isCleanupNotified.get());
    Assert.assertFalse(lowerLock.isCurrentOwner());
    Assert.assertFalse(lowResult);
    lock.unlock();
    lock.close();
    lowerLock.close();
  }

  @Test
  public void testHigherPriorityRequestAcquired() throws Exception {
    ZKDistributedNonblockingLock lock = createLockWithConfig();
    // The waitingTimeout of higher priority request is larger than cleanup time of current owner

    ZKDistributedNonblockingLock.Builder lockBuilder = new ZKDistributedNonblockingLock.Builder();
    lockBuilder.setLockScope(_participantScope).setZkAddress(ZK_ADDR).setTimeout(3600000L)
        .setLockMsg("higher priority lock").setUserId("high_lock").setPriority(2)
        .setWaitingTimeout(30000).setCleanupTimeout(10000).setIsForceful(false)
        .setLockListener(createLockListener());
    ZKDistributedNonblockingLock higherLock = lockBuilder.build();

    Thread t = new Thread() {
      @Override
      public void run() {
        lock.tryLock();
      }
    };

    t.start();
    t.join();
    Assert.assertTrue(lock.isCurrentOwner());

    LockHelper lockHelper = new LockHelper(higherLock);
    Thread t_higher = new Thread(lockHelper);
    t_higher.start();
    t_higher.join();
    boolean higherResult = lockHelper.getResult();

    Assert.assertTrue(_isCleanupNotified.get());
    Assert.assertTrue(higherLock.isCurrentOwner());
    Assert.assertTrue(higherResult);
    _isCleanupNotified.set(false);
    higherLock.unlock();
    higherLock.close();
    lock.close();
  }

  @Test
  public void testHigherPriorityRequestFailedAsCleanupHasNotDone() throws Exception {
    ZKDistributedNonblockingLock lock = createLockWithConfig();
    // The waitingTimeout of higher priority request is shorter than cleanup time of current
    // owner, and the higher priority request is not forceful.
    ZKDistributedNonblockingLock.Builder lockBuilder = new ZKDistributedNonblockingLock.Builder();
    lockBuilder.setLockScope(_participantScope).setZkAddress(ZK_ADDR).setTimeout(3600000L)
        .setLockMsg("higher priority lock short").setUserId("high_lock_short").setPriority(2)
        .setWaitingTimeout(2000).setCleanupTimeout(10000).setIsForceful(false)
        .setLockListener(createLockListener());
    ZKDistributedNonblockingLock higherLock_short = lockBuilder.build();

    Thread t = new Thread() {
      @Override
      public void run() {
        lock.tryLock();
      }
    };

    t.start();
    t.join();
    Assert.assertTrue(lock.isCurrentOwner());

    LockHelper lockHelper = new LockHelper(higherLock_short);
    Thread t_higher = new Thread(lockHelper);

    t_higher.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread th, Throwable ex) {
        Assert.assertTrue(ex.getMessage().contains("Clean up has not finished by lock owner"));
      }
    });
    t_higher.start();
    t_higher.join();
    boolean higherResult = lockHelper.getResult();

    Assert.assertTrue(_isCleanupNotified.get());
    Assert.assertFalse(higherLock_short.isCurrentOwner());
    Assert.assertFalse(higherResult);
    _isCleanupNotified.set(false);

    Assert.assertTrue(lock.unlock());
    Assert.assertFalse(lock.isCurrentOwner());
    lock.close();
    higherLock_short.close();
  }

  @Test
  public void testHigherPriorityRequestForcefulAcquired() throws Exception {
    ZKDistributedNonblockingLock lock = createLockWithConfig();
    // The waitingTimeout of higher priority request is shorter than cleanup time of current
    // owner, but the higher priority request is forceful.
    ZKDistributedNonblockingLock.Builder lockBuilder = new ZKDistributedNonblockingLock.Builder();
    lockBuilder.setLockScope(_participantScope).setZkAddress(ZK_ADDR).setTimeout(3600000L)
        .setLockMsg("higher priority lock force").setUserId("high_lock_force").setPriority(2)
        .setWaitingTimeout(2000).setCleanupTimeout(10000).setIsForceful(true)
        .setLockListener(createLockListener());
    ZKDistributedNonblockingLock higherLock_force = lockBuilder.build();

    Thread t = new Thread() {
      @Override
      public void run() {
        lock.tryLock();
      }
    };

    t.start();
    t.join();
    Assert.assertTrue(lock.isCurrentOwner());

    LockHelper lockHelper = new LockHelper(higherLock_force);
    Thread t_higher = new Thread(lockHelper);

    t_higher.start();
    t_higher.join();
    boolean higherResult = lockHelper.getResult();

    boolean res = TestHelper.verify(() -> {
      return _isCleanupNotified.get();
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(res);
    Assert.assertTrue(higherLock_force.isCurrentOwner());
    Assert.assertTrue(higherResult);
    _isCleanupNotified.set(false);

    higherLock_force.unlock();
    higherLock_force.close();
    lock.close();
  }

  @Test
  public void testHigherPriorityRequestPreemptedByAnother() throws Exception {
    ZKDistributedNonblockingLock lock = createLockWithConfig();

    ZKDistributedNonblockingLock.Builder lockBuilder = new ZKDistributedNonblockingLock.Builder();
    lockBuilder.setLockScope(_participantScope).setZkAddress(ZK_ADDR).setTimeout(3600000L)
        .setLockMsg("higher priority lock").setUserId("high_lock").setPriority(2)
        .setWaitingTimeout(30000).setCleanupTimeout(10000).setIsForceful(false)
        .setLockListener(createLockListener());
    ZKDistributedNonblockingLock higherLock = lockBuilder.build();

    ZKDistributedNonblockingLock.Builder highestLockBuilder =
        new ZKDistributedNonblockingLock.Builder();
    highestLockBuilder.setLockScope(_participantScope).setZkAddress(ZK_ADDR).setTimeout(3600000L)
        .setLockMsg("highest priority lock").setUserId("highest_lock").setPriority(3)
        .setWaitingTimeout(30000).setCleanupTimeout(10000).setIsForceful(false)
        .setLockListener(createLockListener());
    ZKDistributedNonblockingLock highestLock = highestLockBuilder.build();

    Thread t = new Thread() {
      @Override
      public void run() {
        lock.tryLock();
      }
    };

    Thread t_highest = new Thread() {
      @Override
      public void run() {
        highestLock.tryLock();
      }
    };

    t.start();
    t.join();
    Assert.assertTrue(lock.isCurrentOwner());

    LockHelper lockHelper = new LockHelper(higherLock);
    Thread t_higher = new Thread(lockHelper);
    t_higher.start();
    boolean higherResult = TestHelper.verify(() -> {
      return lockHelper.getResult();
    }, 1000);
    Assert.assertFalse(higherResult);
    Assert.assertFalse(higherLock.isCurrentOwner());

    t_highest.start();
    t_highest.join();
    Assert.assertTrue(highestLock.isCurrentOwner());
    _isCleanupNotified.set(false);

    highestLock.unlock();
    highestLock.close();
    higherLock.close();
    lock.close();
  }

  private class LockHelper implements Runnable {
    private volatile boolean result;
    private final ZKDistributedNonblockingLock _lock;

    public LockHelper(ZKDistributedNonblockingLock lock) {
      _lock = lock;
    }

    @Override
    public void run() {
      try {
        result = _lock.tryLock();
      } catch (HelixException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean getResult() {
      return result;
    }
  }

  LockListener createLockListener() {
    return new LockListener() {
      @Override
      public void onCleanupNotification() {
      }
    };
  }

  ZKDistributedNonblockingLock createLockWithConfig() {
    ZKLockConfig.Builder builder = new ZKLockConfig.Builder();
    builder.setLockScope(_participantScope).setZkAdress(ZK_ADDR).setLeaseTimeout(3600000L)
        .setLockMsg("original lock").setUserId("original_lock").setPriority(1)
        .setWaitingTimeout(1000).setCleanupTimeout(25000).setIsForceful(false)
        .setLockListener(_lockListener);
    ZKLockConfig zkLockConfig = builder.build();
    return new ZKDistributedNonblockingLock(zkLockConfig);
  }
}



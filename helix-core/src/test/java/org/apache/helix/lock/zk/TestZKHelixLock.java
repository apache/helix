package org.apache.helix.lock.zk;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.api.Scope;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

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

/**
 * Tests that the Zookeeper-based Helix lock can acquire, block, and release as appropriate
 */
public class TestZKHelixLock extends ZkTestBase {
  @Test
  public void basicTest() throws InterruptedException {
    final long TIMEOUT = 30000;
    final long RETRY_INTERVAL = 100;
    _zkclient.waitUntilConnected(TIMEOUT, TimeUnit.MILLISECONDS);
    final AtomicBoolean t1Locked = new AtomicBoolean(false);
    final AtomicBoolean t1Done = new AtomicBoolean(false);
    final AtomicInteger field1 = new AtomicInteger(0);
    final AtomicInteger field2 = new AtomicInteger(1);
    final ClusterId clusterId = ClusterId.from("testCluster");
    final HelixLock lock1 = new ZKHelixLock(clusterId, Scope.cluster(clusterId), _zkclient);
    final HelixLock lock2 = new ZKHelixLock(clusterId, Scope.cluster(clusterId), _zkclient);

    // thread 1: get a lock, set fields to 1
    Thread t1 = new Thread() {
      @Override
      public void run() {
        lock1.lock();
        synchronized (t1Locked) {
          t1Locked.set(true);
          t1Locked.notify();
        }
        yield(); // if locking doesn't work, t2 will set the fields first
        field1.set(1);
        field2.set(1);
        synchronized (t1Done) {
          t1Done.set(true);
          t1Done.notify();
        }
      }
    };

    // thread 2: wait for t1 to acquire the lock, get a lock, set fields to 2
    Thread t2 = new Thread() {
      @Override
      public void run() {
        synchronized (t1Locked) {
          while (!t1Locked.get()) {
            try {
              t1Locked.wait();
            } catch (InterruptedException e) {
            }
          }
        }
        lock2.lock();
        field1.set(2);
        field2.set(2);
      }
    };

    // start the threads
    t1.setPriority(Thread.MIN_PRIORITY);
    t2.setPriority(Thread.MAX_PRIORITY);
    t1.start();
    t2.start();

    // wait for t1 to finish setting fields
    synchronized (t1Done) {
      while (!t1Done.get()) {
        try {
          t1Done.wait();
        } catch (InterruptedException e) {
        }
      }
    }

    // make sure both fields are 1
    Assert.assertEquals(field1.get(), 1);
    Assert.assertEquals(field2.get(), 1);

    // unlock t1's lock after checking that t2 is blocked
    long count = 0;
    while (!lock2.isBlocked()) {
      if (count > TIMEOUT) {
        break;
      }
      Thread.sleep(RETRY_INTERVAL);
      count += RETRY_INTERVAL;
    }
    Assert.assertTrue(lock2.isBlocked());
    lock1.unlock();

    try {
      // wait for t2, make sure both fields are 2
      t2.join(10000);
      Assert.assertEquals(field1.get(), 2);
      Assert.assertEquals(field2.get(), 2);
    } catch (InterruptedException e) {
    }
  }
}

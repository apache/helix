package org.apache.helix.api.accessor;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.lock.HelixLockable;
import org.apache.helix.lock.zk.ZKHelixLock;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
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
 * Test that the atomic accessors behave atomically in response to interwoven updates.
 */
public class TestAtomicAccessors extends ZkTestBase {
  private static final long TIMEOUT = 30000L;
  private static final long EXTRA_WAIT = 10000L;

  @Test
  public void testClusterUpdates() {
    final ClusterId clusterId = ClusterId.from("TestAtomicAccessors!testCluster");
    final HelixDataAccessor helixAccessor =
        new ZKHelixDataAccessor(clusterId.stringify(), _baseAccessor);
    final LockProvider lockProvider = new LockProvider();
    final String key1 = "key1";
    final String key2 = "key2";

    // set up the cluster (non-atomically since this concurrency comes later)
    ClusterAccessor accessor = new ClusterAccessor(clusterId, helixAccessor);
    ClusterConfig config = new ClusterConfig.Builder(clusterId).build();
    boolean created = accessor.createCluster(config);
    Assert.assertTrue(created);

    // thread that will update the cluster in one way
    Thread t1 = new Thread() {
      @Override
      public void run() {
        UserConfig userConfig = new UserConfig(Scope.cluster(clusterId));
        userConfig.setBooleanField(key1, true);
        ClusterConfig.Delta delta = new ClusterConfig.Delta(clusterId).addUserConfig(userConfig);
        ClusterAccessor accessor =
            new AtomicClusterAccessor(clusterId, helixAccessor, lockProvider);
        accessor.updateCluster(delta);
      }
    };

    // thread that will update the cluster in another way
    Thread t2 = new Thread() {
      @Override
      public void run() {
        UserConfig userConfig = new UserConfig(Scope.cluster(clusterId));
        userConfig.setBooleanField(key2, true);
        ClusterConfig.Delta delta = new ClusterConfig.Delta(clusterId).addUserConfig(userConfig);
        ClusterAccessor accessor =
            new AtomicClusterAccessor(clusterId, helixAccessor, lockProvider);
        accessor.updateCluster(delta);
      }
    };

    // start the threads
    t1.start();
    t2.start();

    // make sure the threads are done
    long startTime = System.currentTimeMillis();
    try {
      t1.join(TIMEOUT);
      t2.join(TIMEOUT);
    } catch (InterruptedException e) {
      Assert.fail(e.getMessage());
      t1.interrupt();
      t2.interrupt();
    }
    long endTime = System.currentTimeMillis();
    if (endTime - startTime > TIMEOUT - EXTRA_WAIT) {
      Assert.fail("Test timed out");
      t1.interrupt();
      t2.interrupt();
    }

    Assert.assertTrue(lockProvider.hasLockBlocked());

    accessor.dropCluster();
  }

  /**
   * A HelixLockable that returns an instrumented ZKHelixLock
   */
  private class LockProvider implements HelixLockable {
    private HelixLock _firstLock = null;
    private AtomicBoolean _hasSecondBlocked = new AtomicBoolean(false);

    @Override
    public synchronized HelixLock getLock(ClusterId clusterId, Scope<?> scope) {
      return new MyLock(clusterId, scope, _zkclient);
    }

    /**
     * Check if a lock object has blocked
     * @return true if a block happened, false otherwise
     */
    public synchronized boolean hasLockBlocked() {
      return _hasSecondBlocked.get();
    }

    /**
     * An instrumented ZKHelixLock
     */
    private class MyLock extends ZKHelixLock {
      /**
       * Instantiate a lock that instruments a ZKHelixLock
       * @param clusterId the cluster to lock
       * @param scope the scope to lock on
       * @param zkClient an active ZooKeeper client
       */
      public MyLock(ClusterId clusterId, Scope<?> scope, ZkClient zkClient) {
        super(clusterId, scope, zkClient);
      }

      @Override
      public synchronized boolean lock() {
        // synchronize here to ensure atomic set and so that the first lock is the first one who
        // gets to lock
        if (_firstLock == null) {
          _firstLock = this;
        }
        return super.lock();
      }

      @Override
      public boolean unlock() {
        if (_firstLock == this) {
          // wait to unlock until another thread has blocked
          synchronized (_hasSecondBlocked) {
            if (!_hasSecondBlocked.get()) {
              try {
                _hasSecondBlocked.wait(TIMEOUT);
              } catch (InterruptedException e) {
              }
            }
          }
        }
        return super.unlock();
      }

      @Override
      protected void setBlocked(boolean isBlocked) {
        if (isBlocked) {
          synchronized (_hasSecondBlocked) {
            _hasSecondBlocked.set(true);
            _hasSecondBlocked.notify();
          }
        }
        super.setBlocked(isBlocked);
      }
    }
  }
}

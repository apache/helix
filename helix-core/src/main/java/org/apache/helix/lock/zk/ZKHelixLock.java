package org.apache.helix.lock.zk;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Scope;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

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
 * Locking scheme for Helix that uses the ZooKeeper exclusive lock implementation
 * Please use the following lock order convention: Cluster, Participant, Resource, Partition
 * WARNING: this is not a reentrant lock
 */
public class ZKHelixLock implements HelixLock {
  private static final Logger LOG = Logger.getLogger(ZKHelixLock.class);

  private static final String LOCK_ROOT = "LOCKS";
  private final String _rootPath;
  private final WriteLock _writeLock;
  private final ZkClient _zkClient;
  private volatile boolean _locked;
  private volatile boolean _canceled;
  private volatile boolean _blocked;

  private final LockListener _listener = new LockListener() {
    @Override
    public void lockReleased() {
    }

    @Override
    public void lockAcquired() {
      synchronized (ZKHelixLock.this) {
        if (!_canceled) {
          _locked = true;
        } else {
          unlock();
        }
        ZKHelixLock.this.notify();
      }
    }
  };

  /**
   * Initialize for a cluster and scope
   * @param clusterId the cluster under which the lock will live
   * @param scope the scope to lock
   * @param zkClient an active ZK client
   */
  public ZKHelixLock(ClusterId clusterId, Scope<?> scope, ZkClient zkClient) {
    _zkClient = zkClient;
    _rootPath =
        '/' + clusterId.stringify() + '/' + LOCK_ROOT + '/' + scope.getType() + '_'
            + scope.getScopedId();
    ZooKeeper zookeeper = ((ZkConnection) zkClient.getConnection()).getZookeeper();
    _writeLock = new WriteLock(zookeeper, _rootPath, null, _listener);
    _locked = false;
    _canceled = false;
    _blocked = false;
  }

  /**
   * Try to synchronously lock the scope
   * @return true if the lock succeeded, false if it failed, as is the case if the connection to ZK
   *         is lost
   */
  @Override
  public synchronized boolean lock() {
    _canceled = false;
    if (_locked || isBlocked()) {
      // no need to proceed if the lock is already acquired or already waiting
      return false;
    }
    try {
      // create the root path if it doesn't exist
      BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);
      baseAccessor.create(_rootPath, null, AccessOption.PERSISTENT);

      // try to acquire the lock
      boolean acquired = _writeLock.lock();
      if (acquired) {
        _locked = true;
      } else {
        setBlocked(true);
        wait();
      }
    } catch (KeeperException e) {
      LOG.error("Error acquiring a lock on " + _rootPath, e);
      _canceled = true;
    } catch (InterruptedException e) {
      LOG.error("Interrupted while acquiring a lock on " + _rootPath);
      _canceled = true;
    }
    setBlocked(false);
    return _locked;
  }

  /**
   * Unlock the scope
   * @return true if unlock executed, false otherwise
   */
  @Override
  public synchronized boolean unlock() {
    try {
      _writeLock.unlock();
    } catch (IllegalArgumentException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Unlock skipped because lock node was not present");
      }
    } catch (RuntimeException e) {
      LOG.error("Error connecting to release the lock");
    }
    _locked = false;
    return true;
  }

  @Override
  public boolean isBlocked() {
    return _blocked;
  }

  /**
   * Set if this the lock method is currently blocked
   * @param isBlocked true if blocked, false otherwise
   */
  protected void setBlocked(boolean isBlocked) {
    _blocked = isBlocked;
  }
}

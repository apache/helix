package org.apache.helix.lock.zk;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.log4j.Logger;

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
 * Helix unblocking lock implementation based on Zookeeper
 */
public class ZKHelixUnblockingLock implements HelixLock {
  private static final Logger LOG = Logger.getLogger(ZKHelixUnblockingLock.class);

  private static final String LOCK_ROOT = "LOCKS";
  private final HelixZkClient _zkClient;
  private final String _lockPath;

  /**
   * Initialize the lock with user provided information, e.g.,cluster, scope, etc.
   * @param clusterName the cluster under which the lock will live
   * @param scope the scope to lock
   * @param zkAddress the zk address the cluster connects to
   * @param timeout the timeout period of the lcok
   * @param lockMsg the reason for having this lock
   */
  public ZKHelixUnblockingLock(String clusterName, HelixConfigScope scope, String zkAddress,
      Long timeout, String lockMsg) {
    this("/" + clusterName + '/' + LOCK_ROOT + '/' + scope, zkAddress, timeout, lockMsg);
  }

  /**
   * Initialize the lock with user provided information, e.g., lock path under zookeeper, etc.
   * @param lockPath the path of the lock under Zookeeper
   * @param zkAddress the zk address of the cluster
   * @param timeout the timeout period of the lcok
   * @param lockMsg the reason for having this lock
   */
  public ZKHelixUnblockingLock(String lockPath, String zkAddress, Long timeout, String lockMsg) {
    _zkClient = new ZkClient(zkAddress);
    _lockPath = lockPath;
  }

  /**
   * Try to acquire the lock
   * @return true if it gets the lock, false if it fails.
   */
  @Override
  public synchronized boolean acquireLock() {
    BaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient.getServers());
    return baseAccessor.create(_lockPath, null, AccessOption.PERSISTENT);
  }

  /**
   * Try to release the lock
   * @return true if the lock is released successfully, false if it fails.
   */
  @Override
  public synchronized boolean releaseLock() {
    Boolean isExist = _zkClient.exists(_lockPath);
    if (isExist) {
      _zkClient.delete(_lockPath);
    }
    return true;
  }

  /**
   * Get the lock metadata information, like timeout, lock message, etc.
   * @return the znode data for this lock
   */
  @Override
  public ZNRecord getLockInfo() {
    BaseDataAccessor<ZNRecord> baseAccessor =
        new ZkBaseDataAccessor<ZNRecord>(_zkClient.getServers());
    return baseAccessor.get(_lockPath, null, AccessOption.PERSISTENT);
  }
}

package org.apache.helix.manager.zk.client;

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

import java.util.HashSet;
import java.util.Set;

import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.zookeeper.Watcher;


/**
 * Deprecated - use ZkConnectionManager in zookeeper-api instead.
 */
@Deprecated
class ZkConnectionManager extends org.apache.helix.zookeeper.impl.factory.ZkConnectionManager {
  /**
   * Construct and init a ZkConnection Manager.
   *  @param zkConnection
   * @param connectionTimeout
   * @param monitorKey
   */
  protected ZkConnectionManager(IZkConnection zkConnection, long connectionTimeout,
      String monitorKey) {
    super(zkConnection, connectionTimeout, monitorKey);
  }

  /**
   * Need to override because of the "instanceof" usage doesn't cover subclasses.
   */
  @Override
  protected void cleanupInactiveWatchers() {
    super.cleanupInactiveWatchers();
    Set<Watcher> closedWatchers = new HashSet<>();
    for (Watcher watcher : _sharedWatchers) {
      // TODO ideally, we shall have a ClosableWatcher interface so as to check accordingly. -- JJ
      if (watcher instanceof SharedZkClient && ((SharedZkClient) watcher).isClosed()) {
        closedWatchers.add(watcher);
      }
    }
    _sharedWatchers.removeAll(closedWatchers);
  }
}

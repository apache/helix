package org.apache.helix.zookeeper.impl.factory;

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

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.client.SharedZkClient;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * NOTE: DO NOT USE THIS CLASS DIRECTLY. Use ZkClientFactories instead.
 *
 * A ZkConnection manager that maintain connection status and allows additional watchers to be registered.
 * It will forward events to those watchers.
 *
 * TODO Separate connection management logic from the raw ZkClient class.
 * So this manager is a peer to the HelixZkClient. Connection Manager for maintaining the connection and
 * HelixZkClient to handle user request.
 * After this is done, Dedicated HelixZkClient hires one manager for it's connection.
 * While multiple Shared ZkClients can use single connection manager if possible.
 */
public class ZkConnectionManager extends ZkClient {
  private static Logger LOG = LoggerFactory.getLogger(ZkConnectionManager.class);
  // Client type that is used in monitor, and metrics.
  private final static String MONITOR_TYPE = "ZkConnectionManager";
  private final String _monitorKey;
  // Set of all registered watchers
  protected final Set<Watcher> _sharedWatchers = new HashSet<>();

  /**
   * Construct and init a ZkConnection Manager.
   *
   * @param zkConnection
   * @param connectionTimeout
   */
  protected ZkConnectionManager(IZkConnection zkConnection, long connectionTimeout,
      String monitorKey) {
    super(zkConnection, (int) connectionTimeout, HelixZkClient.DEFAULT_OPERATION_TIMEOUT,
        new BasicZkSerializer(new SerializableSerializer()), MONITOR_TYPE, monitorKey, null, true);
    _monitorKey = monitorKey;
    LOG.info("ZkConnection {} was created for sharing.", _monitorKey);
  }

  /**
   * Register event watcher.
   *
   * @param watcher
   * @return true if the watcher is newly added. false if it is already in record.
   */
  public synchronized boolean registerWatcher(Watcher watcher) {
    if (isClosed()) {
      throw new ZkClientException("Cannot add watcher to a closed client.");
    }
    return _sharedWatchers.add(watcher);
  }

  /**
   * Unregister the event watcher.
   *
   * @param watcher
   * @return number of the remaining event watchers
   */
  public synchronized int unregisterWatcher(Watcher watcher) {
    _sharedWatchers.remove(watcher);
    return _sharedWatchers.size();
  }

  @Override
  public void process(final WatchedEvent event) {
    super.process(event);
    forwardingEvent(event);
  }

  private synchronized void forwardingEvent(final WatchedEvent event) {
    // note that process (then forwardingEvent) could be triggered during construction, when sharedWatchers is still null.
    if (_sharedWatchers == null || _sharedWatchers.isEmpty()) {
      return;
    }
    // forward event to all the watchers' event queue
    for (final Watcher watcher : _sharedWatchers) {
      watcher.process(event);
    }
  }

  @Override
  public void close() {
    // Enforce closing, if any watcher exists, throw Exception.
    close(false);
  }

  protected synchronized void close(boolean skipIfWatched) {
    cleanupInactiveWatchers();
    if (_sharedWatchers != null && _sharedWatchers.size() > 0) {
      if (skipIfWatched) {
        LOG.debug("Skip closing ZkConnection due to existing watchers. Watcher count {}.",
            _sharedWatchers.size());
        return;
      } else {
        throw new ZkClientException(
            "Cannot close the connection when there are still shared watchers listen on the event.");
      }
    }
    super.close();
    LOG.info("ZkConnection {} was closed.", _monitorKey);
  }

  protected void cleanupInactiveWatchers() {
    Set<Watcher> closedWatchers = new HashSet<>();
    // Null check needed because close() might get invoked before initialization
    if (_sharedWatchers != null) {
      for (Watcher watcher : _sharedWatchers) {
        // TODO ideally, we shall have a ClosableWatcher interface so as to check accordingly. -- JJ
        if (watcher instanceof SharedZkClient && ((SharedZkClient) watcher).isClosed()) {
          closedWatchers.add(watcher);
        }
      }
      _sharedWatchers.removeAll(closedWatchers);
    }
  }
}

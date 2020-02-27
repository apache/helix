package org.apache.helix.rest.metadatastore.concurrency;

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

import java.util.Collections;
import java.util.List;

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkDistributedLeaderElection implements IZkDataListener, IZkStateListener {
  private static final Logger LOG = LoggerFactory.getLogger(ZkDistributedLeaderElection.class);
  private static final String PREFIX = "MSDS_SERVER_";

  private final HelixZkClient _zkClient;
  private final String _basePath;
  private final ZNRecord _participantInfo;
  private ZNRecord _currentLeaderInfo;

  private String _myEphemeralSequentialPath;
  private volatile boolean _isLeader;

  public ZkDistributedLeaderElection(HelixZkClient zkClient, String basePath,
      ZNRecord participantInfo) {
    synchronized (this) {
      if (zkClient == null || zkClient.isClosed()) {
        throw new IllegalArgumentException("ZkClient cannot be null or closed!");
      }
      _zkClient = zkClient;
      _zkClient.setZkSerializer(new ZNRecordSerializer());
      if (basePath == null || basePath.isEmpty()) {
        throw new IllegalArgumentException("lockBasePath cannot be null or empty!");
      }
      _basePath = basePath;
      _participantInfo = participantInfo;
      _isLeader = false;
    }
    init();
  }

  /**
   * Create the base path if it doesn't exist and create an ephemeral sequential ZNode.
   */
  private void init() {
    try {
      _zkClient.createPersistent(_basePath, true);
    } catch (ZkNodeExistsException e) {
      // Okay if it exists already
    }

    // Create my ephemeral sequential node with my information
    _myEphemeralSequentialPath = _zkClient
        .create(_basePath + "/" + PREFIX, _participantInfo, CreateMode.EPHEMERAL_SEQUENTIAL);
    if (_myEphemeralSequentialPath == null) {
      throw new IllegalStateException(
          "Unable to create ephemeral sequential node at path: " + _basePath);
    }
    tryAcquiringLeadership();
  }

  private void tryAcquiringLeadership() {
    List<String> children = _zkClient.getChildren(_basePath);
    Collections.sort(children);
    String leaderName = children.get(0);
    _currentLeaderInfo = _zkClient.readData(_basePath + "/" + leaderName, true);

    String[] myNameArray = _myEphemeralSequentialPath.split("/");
    String myName = myNameArray[myNameArray.length - 1];

    if (leaderName.equals(myName)) {
      // My turn for leadership
      _isLeader = true;
      LOG.info("{} acquired leadership! Info: {}", myName, _currentLeaderInfo);
    } else {
      // Watch the ephemeral ZNode before me for a deletion event
      String beforeMe = children.get(children.indexOf(myName) - 1);
      _zkClient.subscribeDataChanges(_basePath + "/" + beforeMe, this);
    }
  }

  public synchronized boolean isLeader() {
    return _isLeader;
  }

  public synchronized ZNRecord getCurrentLeaderInfo() {
    return _currentLeaderInfo;
  }

  @Override
  public synchronized void handleStateChanged(Watcher.Event.KeeperState state) {
    if (state == Watcher.Event.KeeperState.SyncConnected) {
      init();
    }
  }

  @Override
  public void handleNewSession(String sessionId) {
    return;
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) {
    return;
  }

  @Override
  public void handleDataChange(String s, Object o) {
    return;
  }

  @Override
  public void handleDataDeleted(String s) {
    tryAcquiringLeadership();
  }
}

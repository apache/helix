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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixConnectionStateListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixRole;
import org.apache.helix.HelixService;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;

public class ZkHelixRoleDefaultImpl implements HelixRole, HelixService, HelixConnectionStateListener {
  final ZkHelixConnection _connection;
  final HelixDataAccessor _accessor;
  final BaseDataAccessor<ZNRecord> _baseAccessor;
  final PropertyKey.Builder _keyBuilder;
  final ClusterAccessor _clusterAccessor;
  final ConfigAccessor _configAccessor;
  final ClusterId _clusterId;
  final Id _instanceId;
  final ClusterMessagingService _messagingService;
  boolean _isStarted;

  public ZkHelixRoleDefaultImpl(ZkHelixConnection connection, ClusterId clusterId,
      Id instanceId) {
    _connection = connection;
    _accessor = connection.createDataAccessor(clusterId);
    _baseAccessor = _accessor.getBaseDataAccessor();
    _keyBuilder = _accessor.keyBuilder();
    _clusterAccessor = connection.createClusterAccessor(clusterId);
    _configAccessor = connection.getConfigAccessor();

    _clusterId = clusterId;
    _instanceId = instanceId;
    _messagingService = connection.createMessagingService(this);

  }

  @Override
  public HelixConnection getConnection() {
    return _connection;
  }

  @Override
  public ClusterId getClusterId() {
    return _clusterId;
  }

  @Override
  public Id getId() {
    return _instanceId;
  }

  @Override
  public InstanceType getType() {
    return InstanceType.SPECTATOR;
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    return _messagingService;
  }

  @Override
  public HelixDataAccessor getAccessor() {
    return _accessor;
  }

  @Override
  public void start() {
    _connection.addConnectionStateListener(this);
    if (_connection.isConnected()) {
      onConnected();
    }
  }

  @Override
  public void stop() {
    _connection.removeConnectionStateListener(this);
    onDisconnecting();
  }

  @Override
  public boolean isStarted() {
    return _isStarted;
  }

  @Override
  public void onConnected() {
    _connection.resetHandlers(this);
    _connection.initHandlers(this);
    _isStarted = true;
  }

  @Override
  public void onDisconnecting() {
    _connection.resetHandlers(this);
    _isStarted = false;
  }

}

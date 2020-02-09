package org.apache.helix.rest.server;


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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.InstanceType;
import org.apache.helix.zookeeper.api.datamodel.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.api.ZkClient;
import org.apache.helix.zookeeper.api.HelixZkClient;
import org.apache.helix.zookeeper.api.factory.SharedZkClientFactory;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.zkclient.exception.ZkMarshallingError;
import org.apache.helix.zookeeper.api.zkclient.serialize.ZkSerializer;


public class ServerContext {
  private final String _zkAddr;
  private HelixZkClient _zkClient;
  private ZKHelixAdmin _zkHelixAdmin;
  private ClusterSetup _clusterSetup;
  private ConfigAccessor _configAccessor;
  // A lazily-initialized base data accessor that reads/writes byte array to ZK
  // TODO: Only read (deserialize) is supported at this time. This baseDataAccessor should support write (serialize) as needs arise
  private volatile ZkBaseDataAccessor<byte[]> _byteArrayZkBaseDataAccessor;
  // 1 Cluster name will correspond to 1 helix data accessor
  private final Map<String, HelixDataAccessor> _helixDataAccessorPool;
  // 1 Cluster name will correspond to 1 task driver
  private final Map<String, TaskDriver> _taskDriverPool;

  public ServerContext(String zkAddr) {
    _zkAddr = zkAddr;

    // We should NOT initiate _zkClient and anything that depends on _zkClient in
    // constructor, as it is reasonable to start up HelixRestServer first and then
    // ZooKeeper. In this case, initializing _zkClient will fail and HelixRestServer
    // cannot be started correctly.
    _helixDataAccessorPool = new HashMap<>();
    _taskDriverPool = new HashMap<>();
  }

  public HelixZkClient getHelixZkClient() {
    if (_zkClient == null) {
      HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
      clientConfig.setZkSerializer(new ZNRecordSerializer());
      _zkClient = SharedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(_zkAddr), clientConfig);
    }
    return _zkClient;
  }

  @Deprecated
  public ZkClient getZkClient() {
    return (ZkClient) getHelixZkClient();
  }

  public HelixAdmin getHelixAdmin() {
    if (_zkHelixAdmin == null) {
      _zkHelixAdmin = new ZKHelixAdmin(getHelixZkClient());
    }
    return _zkHelixAdmin;
  }

  public ClusterSetup getClusterSetup() {
    if (_clusterSetup == null) {
      _clusterSetup = new ClusterSetup(getHelixZkClient(), getHelixAdmin());
    }
    return _clusterSetup;
  }

  public TaskDriver getTaskDriver(String clusterName) {
    synchronized (_taskDriverPool) {
      if (!_taskDriverPool.containsKey(clusterName)) {
        _taskDriverPool.put(clusterName, new TaskDriver(getHelixZkClient(), clusterName));
      }
      return _taskDriverPool.get(clusterName);
    }
  }

  public ConfigAccessor getConfigAccessor() {
    if (_configAccessor == null) {
      _configAccessor = new ConfigAccessor(getHelixZkClient());
    }
    return _configAccessor;
  }

  public HelixDataAccessor getDataAccssor(String clusterName) {
    synchronized (_helixDataAccessorPool) {
      if (!_helixDataAccessorPool.containsKey(clusterName)) {
        ZkBaseDataAccessor<ZNRecord> baseDataAccessor =
            new ZkBaseDataAccessor<>(getHelixZkClient());
        _helixDataAccessorPool.put(clusterName,
            new ZKHelixDataAccessor(clusterName, InstanceType.ADMINISTRATOR, baseDataAccessor));
      }
      return _helixDataAccessorPool.get(clusterName);
    }
  }

  /**
   * Returns a lazily-instantiated ZkBaseDataAccessor for the byte array type.
   * @return
   */
  public ZkBaseDataAccessor<byte[]> getByteArrayZkBaseDataAccessor() {
    ZkBaseDataAccessor<byte[]> byteArrayZkBaseDataAccessor = _byteArrayZkBaseDataAccessor;
    if (byteArrayZkBaseDataAccessor != null) { // First check (no locking)
      return byteArrayZkBaseDataAccessor;
    }

    synchronized (this) {
      if (_byteArrayZkBaseDataAccessor == null) { // Second check (with locking)
        _byteArrayZkBaseDataAccessor = new ZkBaseDataAccessor<>(_zkAddr, new ZkSerializer() {
          @Override
          public byte[] serialize(Object o)
              throws ZkMarshallingError {
            // TODO: Support serialize for write methods if necessary
            throw new UnsupportedOperationException("serialize() is not supported.");
          }

          @Override
          public Object deserialize(byte[] bytes)
              throws ZkMarshallingError {
            return bytes;
          }
        });
      }
      return _byteArrayZkBaseDataAccessor;
    }
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
    }
  }
}

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
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.tools.ClusterSetup;

public class ServerContext {
  private final String _zkAddr;
  private final ZkClient _zkClient;
  private final ZKHelixAdmin _zkHelixAdmin;
  private final ClusterSetup _clusterSetup;
  private final ConfigAccessor _configAccessor;

  // 1 Cluster name will correspond to 1 helix data accessor
  private final Map<String, HelixDataAccessor> _helixDataAccessorPool;

  // 1 Cluster name will correspond to 1 task driver
  private final Map<String, TaskDriver> _taskDriverPool;

  public ServerContext(String zkAddr) {
    _zkAddr = zkAddr;
    _zkClient = new ZkClient(_zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());

    // Accessors
    _configAccessor = new ConfigAccessor(getZkClient());
    _helixDataAccessorPool = new HashMap<>();
    _taskDriverPool = new HashMap<>();

    // High level interfaces
    _zkHelixAdmin = new ZKHelixAdmin(getZkClient());
    _clusterSetup = new ClusterSetup(getZkClient(), getHelixAdmin());
  }

  public ZkClient getZkClient() {
    return _zkClient;
  }

  public HelixAdmin getHelixAdmin() {
    return _zkHelixAdmin;
  }

  public ClusterSetup getClusterSetup() {
    return _clusterSetup;
  }

  public TaskDriver getTaskDriver(String clusterName) {
    synchronized (_taskDriverPool) {
      if (!_taskDriverPool.containsKey(clusterName)) {
        _taskDriverPool.put(clusterName, new TaskDriver(getZkClient(), clusterName));
      }
      return _taskDriverPool.get(clusterName);
    }
  }

  public ConfigAccessor getConfigAccessor() {
    return _configAccessor;
  }

  public HelixDataAccessor getDataAccssor(String clusterName) {
    synchronized (_helixDataAccessorPool) {
      if (!_helixDataAccessorPool.containsKey(clusterName)) {
        ZkBaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(getZkClient());
        _helixDataAccessorPool.put(clusterName,
            new ZKHelixDataAccessor(clusterName, InstanceType.ADMINISTRATOR, baseDataAccessor));
      }
      return _helixDataAccessorPool.get(clusterName);
    }
  }

  public void close() {
    if (_zkClient != null) {
      _zkClient.close();
    }
  }
}

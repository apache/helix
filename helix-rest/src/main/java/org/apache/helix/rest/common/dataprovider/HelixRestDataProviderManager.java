package org.apache.helix.rest.common.dataprovider;

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
 *
 */

/** Init, register listener and manager callback handler for different
 * clusters manage the providers lifecycle
 *
 */

import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;


public class HelixRestDataProviderManager {

  protected RealmAwareZkClient _zkclient;

  private HelixAdmin _helixAdmin;

  private RestServiceDataProvider _restServiceDataProvider;
  // list of callback handlers

  //TODO: create own zk client
  public HelixRestDataProviderManager(RealmAwareZkClient zkclient, HelixAdmin helixAdmin) {
    _zkclient = zkclient;
    _helixAdmin = helixAdmin;
    _restServiceDataProvider = new RestServiceDataProvider();
    init();
  }

  public RestServiceDataProvider getRestServiceDataProvider() {
    return _restServiceDataProvider;
  }

  private void init() {
    List<String> clusters = _helixAdmin.getClusters();
    for (String cluster : clusters) {
      PerClusterDataProvider clusterDataProvider =
          new PerClusterDataProvider(cluster, _zkclient, new ZkBaseDataAccessor(_zkclient));
      clusterDataProvider.initCache();
      // register callback handler for each provider
      _restServiceDataProvider.addClusterDataProvider(cluster, clusterDataProvider);
    }
  }

  public void close() {
  }
}

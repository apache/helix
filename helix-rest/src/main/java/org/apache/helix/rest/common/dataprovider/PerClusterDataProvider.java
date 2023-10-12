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
 */

import java.util.Map;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.caches.PropertyCache;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;


/**
 * Dara cache for each Helix cluster. Configs, ideal stats and current states are read from ZK and updated
 * using event changes. External view are consolidated using current state.
 */
public class PerClusterDataProvider {

  private HelixDataAccessor _accessor;

  private RealmAwareZkClient _zkclient;

  private final String _clusterName;

  // Simple caches
  private final RestPropertyCache<InstanceConfig> _instanceConfigCache;
  private final RestPropertyCache<ClusterConfig> _clusterConfigCache;
  private final RestPropertyCache<ResourceConfig> _resourceConfigCache;
  private final RestPropertyCache<LiveInstance> _liveInstanceCache;
  private final RestPropertyCache<IdealState> _idealStateCache;
  private final RestPropertyCache<StateModelDefinition> _stateModelDefinitionCache;

  // special caches
  private final RestCurrentStateCache _currentStateCache;

  // TODO: add external view caches

  public PerClusterDataProvider(String clusterName, RealmAwareZkClient zkClient, BaseDataAccessor baseDataAccessor) {
    _clusterName = clusterName;
    _accessor = new ZKHelixDataAccessor(clusterName, baseDataAccessor);

    _zkclient = zkClient;
    _instanceConfigCache = null;
    _clusterConfigCache = null;
    _resourceConfigCache = null;
    _liveInstanceCache = null;
    _idealStateCache = null;
    _stateModelDefinitionCache = null;
    _currentStateCache = null;
  }
  // TODO: consolidate EV from CSs
  public Map<String, ExternalView> consolidateExternalViews() {
    return null;
  }

  // Used for dummy cache. Remove later
  public void initCache(final HelixDataAccessor accessor) {

  }

  public void initCache() {

  }
}

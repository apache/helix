package org.apache.helix.ui.util;

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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.ui.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class DataCache {
  private static final Logger LOG = LoggerFactory.getLogger(DataCache.class);
  private static final int CACHE_EXPIRY_TIME = 30;
  private static final TimeUnit CACHE_EXPIRY_UNIT = TimeUnit.SECONDS;

  private final LoadingCache<String, List<String>> clusterCache;
  private final LoadingCache<ClusterSpec, List<String>> resourceCache;
  private final LoadingCache<ClusterSpec, List<ConfigTableRow>> configCache;
  private final LoadingCache<ResourceSpec, List<ConfigTableRow>> resourceConfigCache;
  private final LoadingCache<ClusterSpec, List<InstanceSpec>> instanceCache;

  public DataCache(final ClientCache clientCache) {
    this.clusterCache = CacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_EXPIRY_TIME, CACHE_EXPIRY_UNIT)
            .build(new CacheLoader<String, List<String>>() {
              @Override
              public List<String> load(String zkAddress) throws Exception {
                ZkClient zkClient = clientCache.get(zkAddress).getZkClient();
                List<String> clusters = zkClient.getChildren("/");
                Collections.sort(clusters);
                return clusters;
              }
            });

    this.resourceCache = CacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_EXPIRY_TIME, CACHE_EXPIRY_UNIT)
            .build(new CacheLoader<ClusterSpec, List<String>>() {
              @Override
              public List<String> load(ClusterSpec clusterSpec) throws Exception {
                ClusterSetup clusterSetup = clientCache.get(clusterSpec.getZkAddress()).getClusterSetup();
                List<String> resources = new ArrayList<String>(clusterSetup.getClusterManagementTool().getResourcesInCluster(clusterSpec.getClusterName()));
                Collections.sort(resources);
                return resources;
              }
            });

    this.configCache = CacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_EXPIRY_TIME, CACHE_EXPIRY_UNIT)
            .build(new CacheLoader<ClusterSpec, List<ConfigTableRow>>() {
              @Override
              public List<ConfigTableRow> load(ClusterSpec clusterSpec) throws Exception {
                ClusterSetup clusterSetup = clientCache.get(clusterSpec.getZkAddress()).getClusterSetup();
                List<ConfigTableRow> configTable = new ArrayList<ConfigTableRow>();

                // Cluster config
                HelixConfigScope configScope
                        = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
                        .forCluster(clusterSpec.getClusterName()).build();
                List<String> clusterConfigKeys
                        = clusterSetup.getClusterManagementTool().getConfigKeys(configScope);
                Map<String, String> config
                        = clusterSetup.getClusterManagementTool().getConfig(configScope, clusterConfigKeys);
                for (Map.Entry<String, String> entry : config.entrySet()) {
                  configTable.add(new ConfigTableRow(
                          HelixConfigScope.ConfigScopeProperty.CLUSTER.toString(),
                          clusterSpec.getClusterName(),
                          entry.getKey(),
                          entry.getValue()));
                }

                Collections.sort(configTable);

                return configTable;
              }
            });

    this.resourceConfigCache = CacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_EXPIRY_TIME, CACHE_EXPIRY_UNIT)
            .build(new CacheLoader<ResourceSpec, List<ConfigTableRow>>() {
              @Override
              public List<ConfigTableRow> load(ResourceSpec resourceSpec) throws Exception {
                ClusterSetup clusterSetup = clientCache.get(resourceSpec.getZkAddress()).getClusterSetup();
                List<ConfigTableRow> configTable = new ArrayList<ConfigTableRow>();

                HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.RESOURCE)
                        .forCluster(resourceSpec.getClusterName())
                        .forResource(resourceSpec.getResourceName())
                        .build();
                try {
                  List<String> clusterConfigKeys = clusterSetup.getClusterManagementTool().getConfigKeys(configScope);

                  Map<String, String> config = clusterSetup.getClusterManagementTool().getConfig(configScope, clusterConfigKeys);

                  if (config != null) {
                    for (Map.Entry<String, String> entry : config.entrySet()) {
                      configTable.add(new ConfigTableRow(
                              HelixConfigScope.ConfigScopeProperty.RESOURCE.toString(),
                              resourceSpec.getClusterName(),
                              entry.getKey(),
                              entry.getValue()));
                    }
                  }
                } catch (Exception e) {
                  LOG.warn("Could not get resource config for {}", resourceSpec.getResourceName(), e);
                }

                return configTable;
              }
            });

    this.instanceCache = CacheBuilder.newBuilder()
            .expireAfterWrite(CACHE_EXPIRY_TIME, CACHE_EXPIRY_UNIT)
            .build(new CacheLoader<ClusterSpec, List<InstanceSpec>>() {
              @Override
              public List<InstanceSpec> load(ClusterSpec clusterSpec) throws Exception {
                ClusterConnection clusterConnection = clientCache.get(clusterSpec.getZkAddress());

                // Instances in the cluster
                List<String> instances =
                        clusterConnection.getClusterSetup().getClusterManagementTool().getInstancesInCluster(clusterSpec.getClusterName());

                // Live instances in the cluster
                // TODO: should be able to use clusterSetup for this, but no method available
                List<String> liveInstances
                        = clusterConnection.getZkClient().getChildren(String.format("/%s/LIVEINSTANCES", clusterSpec.getClusterName()));
                Set<String> liveInstanceSet = new HashSet<String>();
                if (liveInstances != null) {
                  liveInstanceSet.addAll(liveInstances);
                }

                // Enabled instances
                Set<String> enabledInstances = new HashSet<String>();
                if (instances != null) {
                  for (String instance : instances) {
                    InstanceConfig instanceConfig = clusterConnection.getClusterSetup()
                            .getClusterManagementTool()
                            .getInstanceConfig(clusterSpec.getClusterName(), instance);
                    if (instanceConfig.getInstanceEnabled()) {
                      enabledInstances.add(instance);
                    }
                  }
                }

                // Rows
                List<InstanceSpec> instanceSpecs = new ArrayList<InstanceSpec>();
                if (instances != null) {
                  for (String instance : instances) {
                    instanceSpecs.add(new InstanceSpec(
                            instance,
                            enabledInstances.contains(instance),
                            liveInstanceSet.contains(instance)));
                  }
                }

                return instanceSpecs;
              }
            });
  }

  public void invalidate() {
    clusterCache.invalidateAll();
    resourceCache.invalidateAll();
    configCache.invalidateAll();
    resourceConfigCache.invalidateAll();
    instanceCache.invalidateAll();
  }

  public LoadingCache<String, List<String>> getClusterCache() {
    return clusterCache;
  }

  public LoadingCache<ClusterSpec, List<String>> getResourceCache() {
    return resourceCache;
  }

  public LoadingCache<ClusterSpec, List<ConfigTableRow>> getConfigCache() {
    return configCache;
  }

  public LoadingCache<ResourceSpec, List<ConfigTableRow>> getResourceConfigCache() {
    return resourceConfigCache;
  }

  public LoadingCache<ClusterSpec, List<InstanceSpec>> getInstanceCache() {
    return instanceCache;
  }
}

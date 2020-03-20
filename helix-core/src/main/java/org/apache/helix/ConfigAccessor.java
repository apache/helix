package org.apache.helix;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.manager.zk.GenericZkHelixApiBuilder;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.RESTConfig;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.util.StringTemplate;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides access to the persistent configuration of the cluster, the instances that live on it,
 * and the logical resources assigned to it.
 */
public class ConfigAccessor {
  private static Logger LOG = LoggerFactory.getLogger(ConfigAccessor.class);

  private static final StringTemplate template = new StringTemplate();

  static {
    // @formatter:off
    template.addEntry(ConfigScopeProperty.CLUSTER, 1, "/{clusterName}/CONFIGS/CLUSTER");
    template.addEntry(ConfigScopeProperty.CLUSTER, 2,
        "/{clusterName}/CONFIGS/CLUSTER/{clusterName}|SIMPLEKEYS");
    template.addEntry(ConfigScopeProperty.PARTICIPANT, 1, "/{clusterName}/CONFIGS/PARTICIPANT");
    template.addEntry(ConfigScopeProperty.PARTICIPANT, 2,
        "/{clusterName}/CONFIGS/PARTICIPANT/{participantName}|SIMPLEKEYS");
    template.addEntry(ConfigScopeProperty.RESOURCE, 1, "/{clusterName}/CONFIGS/RESOURCE");
    template.addEntry(ConfigScopeProperty.RESOURCE, 2,
        "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|SIMPLEKEYS");
    template.addEntry(ConfigScopeProperty.PARTITION, 2,
        "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|MAPKEYS");
    template.addEntry(ConfigScopeProperty.PARTITION, 3,
        "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|MAPMAPKEYS|{partitionName}");
    // @formatter:on
  }

  private final RealmAwareZkClient _zkClient;
  // true if ConfigAccessor was instantiated with a HelixZkClient, false otherwise
  // This is used for close() to determine how ConfigAccessor should close the underlying ZkClient
  private final boolean _usesExternalZkClient;

  private ConfigAccessor(RealmAwareZkClient zkClient, boolean usesExternalZkClient) {
    _zkClient = zkClient;
    _usesExternalZkClient = usesExternalZkClient;
  }

  /**
   * Initialize an accessor with a Zookeeper client
   * Note: it is recommended to use the other constructor instead to avoid having to create a
   * RealmAwareZkClient.
   * @param zkClient
   */
  @Deprecated
  public ConfigAccessor(RealmAwareZkClient zkClient) {
    _zkClient = zkClient;
    _usesExternalZkClient = true;
  }

  /**
   * Initialize a ConfigAccessor with a ZooKeeper connect string. It will use a SharedZkClient with
   * default settings. Note that ZNRecordSerializer will be used for the internal ZkClient since
   * ConfigAccessor only deals with Helix's data models like ResourceConfig.
   * @param zkAddress
   */
  @Deprecated
  public ConfigAccessor(String zkAddress) {
    _usesExternalZkClient = false;

    // If the multi ZK config is enabled, use FederatedZkClient on multi-realm mode
    if (Boolean.parseBoolean(System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED))) {
      try {
        _zkClient = new FederatedZkClient(
            new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build(),
            new RealmAwareZkClient.RealmAwareZkClientConfig()
                .setZkSerializer(new ZNRecordSerializer()));
        return;
      } catch (IOException | InvalidRoutingDataException | IllegalStateException e) {
        throw new HelixException("Failed to create ConfigAccessor!", e);
      }
    }

    _zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
            new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));
  }

  /**
   * get config
   * @deprecated replaced by {@link #get(HelixConfigScope, String)}
   * @param scope
   * @param key
   * @return value or null if doesn't exist
   */
  @Deprecated
  public String get(ConfigScope scope, String key) {
    Map<String, String> map = get(scope, Arrays.asList(key));
    return map.get(key);
  }

  /**
   * get configs
   * @deprecated replaced by {@link #get(HelixConfigScope, List<String>)}
   * @param scope
   * @param keys
   * @return
   */
  @Deprecated
  public Map<String, String> get(ConfigScope scope, List<String> keys) {
    if (scope == null || scope.getScope() == null) {
      LOG.error("Scope can't be null");
      return null;
    }

    // String value = null;
    Map<String, String> map = new HashMap<String, String>();
    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    String scopeStr = scope.getScopeStr();
    String[] splits = scopeStr.split("\\|");

    ZNRecord record = _zkClient.readData(splits[0], true);

    if (record != null) {
      if (splits.length == 1) {
        for (String key : keys) {
          if (record.getSimpleFields().containsKey(key)) {
            map.put(key, record.getSimpleField(key));
          }
        }
      } else if (splits.length == 2) {
        if (record.getMapField(splits[1]) != null) {
          for (String key : keys) {
            if (record.getMapField(splits[1]).containsKey(key)) {
              map.put(key, record.getMapField(splits[1]).get(key));
            }
          }
        }
      }
    }
    return map;
  }

  /**
   * get a single config entry
   * @param scope specification of the entity set to query
   *          (e.g. cluster, resource, participant, etc.)
   * @param key the identifier of the configuration entry
   * @return the configuration entry
   */
  public String get(HelixConfigScope scope, String key) {
    Map<String, String> map = get(scope, Arrays.asList(key));
    if (map != null) {
      return map.get(key);
    }
    return null;
  }

  /**
   * get many config entries
   * @param scope scope specification of the entity set to query
   *          (e.g. cluster, resource, participant, etc.)
   * @param keys the identifiers of the configuration entries
   * @return the configuration entries, organized by key
   */
  public Map<String, String> get(HelixConfigScope scope, List<String> keys) {
    if (scope == null || scope.getType() == null || !scope.isFullKey()) {
      LOG.error("fail to get configs. invalid config scope. scope: {}, keys: {}.", scope, keys);
      return null;
    }
    ZNRecord record = getConfigZnRecord(scope);

    if (record == null) {
      LOG.warn("No config found at {}.", scope.getZkPath());
      return null;
    }

    Map<String, String> map = new HashMap<String, String>();
    String mapKey = scope.getMapKey();
    if (mapKey == null) {
      for (String key : keys) {
        if (record.getSimpleFields().containsKey(key)) {
          map.put(key, record.getSimpleField(key));
        }
      }
    } else {
      Map<String, String> configMap = record.getMapField(mapKey);
      if (configMap == null) {
        LOG.warn("No map-field found in {} using mapKey: {}.", record, mapKey);
        return null;
      }

      for (String key : keys) {
        if (record.getMapField(mapKey).containsKey(key)) {
          map.put(key, record.getMapField(mapKey).get(key));
        }
      }
    }

    return map;
  }

  private ZNRecord getConfigZnRecord(HelixConfigScope scope) {
    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("fail to get configs. cluster " + clusterName + " is not setup yet");
    }

    return _zkClient.readData(scope.getZkPath(), true);
  }

  /**
   * Set config, create if not exist
   * @deprecated replaced by {@link #set(HelixConfigScope, String, String)}
   * @param scope
   * @param key
   * @param value
   */
  @Deprecated
  public void set(ConfigScope scope, String key, String value) {
    Map<String, String> map = new HashMap<String, String>();
    map.put(key, value);
    set(scope, map);
  }

  /**
   * Set configs, create if not exist
   * @deprecated replaced by {@link #set(HelixConfigScope, Map<String, String>)}
   * @param scope
   * @param keyValueMap
   */
  @Deprecated
  public void set(ConfigScope scope, Map<String, String> keyValueMap) {
    if (scope == null || scope.getScope() == null) {
      LOG.error("Scope can't be null.");
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster: " + clusterName + " is NOT setup.");
    }

    if (scope.getScope() == ConfigScopeProperty.PARTICIPANT) {
      String scopeStr = scope.getScopeStr();
      String instanceName = scopeStr.substring(scopeStr.lastIndexOf('/') + 1);
      if (!ZKUtil.isInstanceSetup(_zkClient, scope.getClusterName(), instanceName,
          InstanceType.PARTICIPANT)) {
        throw new HelixException(
            "instance: " + instanceName + " is NOT setup in cluster: " + clusterName);
      }
    }

    // use "|" to delimit resource and partition. e.g. /MyCluster/CONFIGS/PARTICIPANT/MyDB|MyDB_0
    String scopeStr = scope.getScopeStr();
    String[] splits = scopeStr.split("\\|");

    String id = splits[0].substring(splits[0].lastIndexOf('/') + 1);
    ZNRecord update = new ZNRecord(id);
    if (splits.length == 1) {
      for (String key : keyValueMap.keySet()) {
        String value = keyValueMap.get(key);
        update.setSimpleField(key, value);
      }
    } else if (splits.length == 2) {
      if (update.getMapField(splits[1]) == null) {
        update.setMapField(splits[1], new TreeMap<String, String>());
      }
      for (String key : keyValueMap.keySet()) {
        String value = keyValueMap.get(key);
        update.getMapField(splits[1]).put(key, value);
      }
    }
    ZKUtil.createOrMerge(_zkClient, splits[0], update, true, true);
  }

  /**
   * Set config, creating it if it doesn't exist
   * @param scope scope specification of the entity set to query
   *          (e.g. cluster, resource, participant, etc.)
   * @param key the identifier of the configuration entry
   * @param value the configuration
   */
  public void set(HelixConfigScope scope, String key, String value) {
    Map<String, String> map = new TreeMap<String, String>();
    map.put(key, value);
    set(scope, map);
  }

  /**
   * Set multiple configs, creating them if they don't exist
   * @param scope scope specification of the entity set to query
   *          (e.g. cluster, resource, participant, etc.)
   * @param keyValueMap configurations organized by their identifiers
   */
  public void set(HelixConfigScope scope, Map<String, String> keyValueMap) {
    if (scope == null || scope.getType() == null || !scope.isFullKey()) {
      LOG.error("fail to set config. invalid config scope. Scope: {}.", scope);
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("fail to set config. cluster: " + clusterName + " is NOT setup.");
    }

    if (scope.getType() == ConfigScopeProperty.PARTICIPANT) {
      if (!ZKUtil.isInstanceSetup(_zkClient, scope.getClusterName(), scope.getParticipantName(),
          InstanceType.PARTICIPANT)) {
        throw new HelixException("fail to set config. instance: " + scope.getParticipantName()
            + " is NOT setup in cluster: " + clusterName);
      }
    }

    String mapKey = scope.getMapKey();
    String zkPath = scope.getZkPath();
    String id = zkPath.substring(zkPath.lastIndexOf('/') + 1);
    ZNRecord update = new ZNRecord(id);
    if (mapKey == null) {
      update.getSimpleFields().putAll(keyValueMap);
    } else {
      update.setMapField(mapKey, keyValueMap);
    }

    ZKUtil.createOrMerge(_zkClient, zkPath, update, true, true);
  }

  /**
   * Remove config
   * @deprecated replaced by {@link #remove(HelixConfigScope, String)}
   * @param scope
   * @param key
   */
  @Deprecated
  public void remove(ConfigScope scope, String key) {
    remove(scope, Arrays.asList(key));
  }

  /**
   * remove configs
   * @deprecated replaced by {@link #remove(HelixConfigScope, List<String>)}
   * @param scope
   * @param keys
   */
  @Deprecated
  public void remove(ConfigScope scope, List<String> keys) {
    if (scope == null || scope.getScope() == null) {
      LOG.error("Scope can't be null.");
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("cluster " + clusterName + " is not setup yet");
    }

    String scopeStr = scope.getScopeStr();
    String[] splits = scopeStr.split("\\|");

    String id = splits[0].substring(splits[0].lastIndexOf('/') + 1);
    ZNRecord update = new ZNRecord(id);
    if (splits.length == 1) {
      // subtract doesn't care about value, use empty string
      for (String key : keys) {
        update.setSimpleField(key, "");
      }
    } else if (splits.length == 2) {
      if (update.getMapField(splits[1]) == null) {
        update.setMapField(splits[1], new TreeMap<String, String>());
      }
      // subtract doesn't care about value, use empty string
      for (String key : keys) {
        update.getMapField(splits[1]).put(key, "");
      }
    }

    ZKUtil.subtract(_zkClient, splits[0], update);
  }

  /**
   * Remove a single config
   * @param scope scope specification of the entity set to query
   *          (e.g. cluster, resource, participant, etc.)
   * @param key the identifier of the configuration entry
   */
  public void remove(HelixConfigScope scope, String key) {
    remove(scope, Arrays.asList(key));
  }

  /**
   * Remove multiple configs
   * @param scope scope specification of the entity set to query
   *          (e.g. cluster, resource, participant, etc.)
   * @param keys the identifiers of the configuration entries
   */
  public void remove(HelixConfigScope scope, List<String> keys) {
    if (scope == null || scope.getType() == null || !scope.isFullKey()) {
      LOG.error("fail to remove. invalid scope: {}, keys: {}", scope, keys);
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("fail to remove. cluster " + clusterName + " is not setup yet");
    }

    String zkPath = scope.getZkPath();
    String mapKey = scope.getMapKey();
    String id = zkPath.substring(zkPath.lastIndexOf('/') + 1);
    ZNRecord update = new ZNRecord(id);
    if (mapKey == null) {
      // subtract doesn't care about value, use empty string
      for (String key : keys) {
        update.setSimpleField(key, "");
      }
    } else {
      update.setMapField(mapKey, new TreeMap<String, String>());
      // subtract doesn't care about value, use empty string
      for (String key : keys) {
        update.getMapField(mapKey).put(key, "");
      }
    }

    ZKUtil.subtract(_zkClient, zkPath, update);
  }

  /**
   * Remove multiple configs
   *
   * @param scope          scope specification of the entity set to query (e.g. cluster, resource,
   *                       participant, etc.)
   * @param recordToRemove the ZNRecord that holds the entries that needs to be removed
   */
  public void remove(HelixConfigScope scope, ZNRecord recordToRemove) {
    if (scope == null || scope.getType() == null || !scope.isFullKey()) {
      LOG.error("fail to remove. invalid scope: {}.", scope);
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("fail to remove. cluster " + clusterName + " is not setup yet");
    }

    String zkPath = scope.getZkPath();
    ZKUtil.subtract(_zkClient, zkPath, recordToRemove);
  }

  /**
   * get config keys
   * @deprecated replaced by {@link #getKeys(HelixConfigScope)}
   * @param type
   * @param clusterName
   * @param keys
   * @return
   */
  @Deprecated
  public List<String> getKeys(ConfigScopeProperty type, String clusterName, String... keys) {
    if (type == null || clusterName == null) {
      LOG.error("ClusterName|scope can't be null.");
      return Collections.emptyList();
    }

    try {
      if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
        LOG.error("cluster {} is not setup yet.", clusterName);
        return Collections.emptyList();
      }

      String[] args = new String[1 + keys.length];
      args[0] = clusterName;
      System.arraycopy(keys, 0, args, 1, keys.length);
      String scopeStr = template.instantiate(type, args);
      String[] splits = scopeStr.split("\\|");
      List<String> retKeys = null;
      if (splits.length == 1) {
        retKeys = _zkClient.getChildren(splits[0]);
      } else {
        ZNRecord record = _zkClient.readData(splits[0]);

        if (splits[1].startsWith("SIMPLEKEYS")) {
          retKeys = new ArrayList<String>(record.getSimpleFields().keySet());
        } else if (splits[1].startsWith("MAPKEYS")) {
          retKeys = new ArrayList<String>(record.getMapFields().keySet());
        } else if (splits[1].startsWith("MAPMAPKEYS")) {
          retKeys = new ArrayList<String>(record.getMapField(splits[2]).keySet());
        }
      }
      if (retKeys == null) {
        LOG.error("Invalid scope: {} or keys: {}.", type, Arrays.toString(args));
        return Collections.emptyList();
      }

      Collections.sort(retKeys);
      return retKeys;
    } catch (Exception e) {
      return Collections.emptyList();
    }
  }

  /**
   * Get list of config keys for a scope
   * @param scope
   * @return a list of configuration keys
   */
  public List<String> getKeys(HelixConfigScope scope) {
    if (scope == null || scope.getType() == null) {
      LOG.error("Fail to getKeys. Invalid config scope: {}.", scope);
      return null;
    }

    if (!ZKUtil.isClusterSetup(scope.getClusterName(), _zkClient)) {
      LOG.error("Fail to getKeys. Cluster {} is not setup yet.", scope.getClusterName());
      return Collections.emptyList();
    }

    String zkPath = scope.getZkPath();
    String mapKey = scope.getMapKey();
    List<String> retKeys = null;

    if (scope.isFullKey()) {
      ZNRecord record = _zkClient.readData(zkPath);
      if (mapKey == null) {
        retKeys = new ArrayList<String>(record.getSimpleFields().keySet());
      } else {
        retKeys = new ArrayList<String>(record.getMapField(mapKey).keySet());
      }
    } else {
      if (scope.getType() == ConfigScopeProperty.PARTITION) {
        ZNRecord record = _zkClient.readData(zkPath);
        retKeys = new ArrayList<String>(record.getMapFields().keySet());
      } else {
        retKeys = _zkClient.getChildren(zkPath);
      }
    }

    if (retKeys != null) {
      Collections.sort(retKeys);
    }
    return retKeys;
  }

  /**
   * Get ClusterConfig of the given cluster.
   *
   * @param clusterName
   *
   * @return
   */
  public ClusterConfig getClusterConfig(String clusterName) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("fail to get config. cluster: " + clusterName + " is NOT setup.");
    }

    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    ZNRecord record = getConfigZnRecord(scope);

    if (record == null) {
      LOG.warn("No config found at {}.", scope.getZkPath());
      return null;
    }

    return new ClusterConfig(record);
  }

  /**
   * Get RestConfig of the given cluster.
   *
   * @param clusterName The cluster
   *
   * @return The instance of {@link RESTConfig}
   */
  public RESTConfig getRESTConfig(String clusterName) {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.REST).forCluster(clusterName).build();
    ZNRecord record = getConfigZnRecord(scope);

    if (record == null) {
      LOG.warn("No rest config found at {}.", scope.getZkPath());
      return null;
    }

    return new RESTConfig(record);
  }

  /**
   * Set RestConfig of a given cluster
   * @param clusterName the cluster id
   * @param restConfig the RestConfig to be set to the cluster
   */
  public void setRESTConfig(String clusterName, RESTConfig restConfig) {
    updateRESTConfig(clusterName, restConfig, true);
  }

  /**
   * Update RestConfig of a given cluster
   * @param clusterName the cluster id
   * @param restConfig the new RestConfig to be set to the cluster
   */
  public void updateRESTConfig(String clusterName, RESTConfig restConfig) {
    updateRESTConfig(clusterName, restConfig, false);
  }

  private void updateRESTConfig(String clusterName, RESTConfig restConfig, boolean overwrite) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("Fail to update REST config. cluster: " + clusterName + " is NOT setup.");
    }

    HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.REST).forCluster(clusterName).build();
    String zkPath = scope.getZkPath();

    // Create "/{clusterId}/CONFIGS/REST" if it does not exist
    String parentPath = HelixUtil.getZkParentPath(zkPath);
    if (!_zkClient.exists(parentPath)) {
      ZKUtil.createOrMerge(_zkClient, parentPath, new ZNRecord(parentPath), true, true);
    }

    if (overwrite) {
      ZKUtil.createOrReplace(_zkClient, zkPath, restConfig.getRecord(), true);
    } else {
      ZKUtil.createOrUpdate(_zkClient, zkPath, restConfig.getRecord(), true, true);
    }
  }

  public void deleteRESTConfig(String clusterName) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("Fail to delete REST config. cluster: " + clusterName + " is NOT setup.");
    }

    HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.REST).forCluster(clusterName).build();
    String zkPath = scope.getZkPath();

    // Check if "/{clusterId}/CONFIGS/REST" exists
    String parentPath = HelixUtil.getZkParentPath(zkPath);
    if (!_zkClient.exists(parentPath)) {
      throw new HelixException("Fail to delete REST config. cluster: " + clusterName + " does not have a rest config.");    }

    ZKUtil.dropChildren(_zkClient, parentPath, new ZNRecord(clusterName));
  }

  /**
   * Set ClusterConfig of the given cluster.
   * The current Cluster config will be replaced with the given clusterConfig.
   * WARNING: This is not thread-safe or concurrent updates safe.
   *
   * @param clusterName
   * @param clusterConfig
   *
   * @return
   */
  public void setClusterConfig(String clusterName, ClusterConfig clusterConfig) {
    updateClusterConfig(clusterName, clusterConfig, true);
  }

  /**
   * Update ClusterConfig of the given cluster.
   * The value of field in current config will be replaced with the value of the same field in given config if it
   * presents. If there is new field in given config but not in current config, the field will be added into
   * the current config..
   * The list fields and map fields will be replaced as a single entry.
   *
   * The current Cluster config will be replaced with the given clusterConfig.
   * WARNING: This is not thread-safe or concurrent updates safe.
   *
   * @param clusterName
   * @param clusterConfig
   *
   * @return
   */
  public void updateClusterConfig(String clusterName, ClusterConfig clusterConfig) {
    updateClusterConfig(clusterName, clusterConfig, false);
  }

  private void updateClusterConfig(String clusterName, ClusterConfig clusterConfig,
      boolean overwrite) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("fail to update config. cluster: " + clusterName + " is NOT setup.");
    }

    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();
    String zkPath = scope.getZkPath();

    if (overwrite) {
      ZKUtil.createOrReplace(_zkClient, zkPath, clusterConfig.getRecord(), true);
    } else {
      ZKUtil.createOrUpdate(_zkClient, zkPath, clusterConfig.getRecord(), true, true);
    }
  }

  /**
   * Get resource config for given resource in given cluster.
   *
   * @param clusterName
   * @param resourceName
   *
   * @return
   */
  public ResourceConfig getResourceConfig(String clusterName, String resourceName) {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(clusterName)
            .forResource(resourceName).build();
    ZNRecord record = getConfigZnRecord(scope);

    if (record == null) {
      LOG.warn("No config found at {}.", scope.getZkPath());
      return null;
    }

    return new ResourceConfig(record);
  }

  /**
   * Set config of the given resource.
   * The current Resource config will be replaced with the given clusterConfig.
   *
   * WARNING: This is not thread-safe or concurrent updates safe.
   *
   * @param clusterName
   * @param resourceName
   * @param resourceConfig
   *
   * @return
   */
  public void setResourceConfig(String clusterName, String resourceName,
      ResourceConfig resourceConfig) {
    updateResourceConfig(clusterName, resourceName, resourceConfig, true);
  }

  /**
   * Update ResourceConfig of the given resource.
   * The value of field in current config will be replaced with the value of the same field in given config if it
   * presents. If there is new field in given config but not in current config, the field will be added into
   * the current config..
   * The list fields and map fields will be replaced as a single entry.
   *
   * The current Cluster config will be replaced with the given clusterConfig.
   * WARNING: This is not thread-safe or concurrent updates safe.
   *
   * @param clusterName
   * @param resourceName
   * @param resourceConfig
   *
   * @return
   */
  public void updateResourceConfig(String clusterName, String resourceName,
      ResourceConfig resourceConfig) {
    updateResourceConfig(clusterName, resourceName, resourceConfig, false);
  }

  private void updateResourceConfig(String clusterName, String resourceName,
      ResourceConfig resourceConfig, boolean overwrite) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("fail to setup config. cluster: " + clusterName + " is NOT setup.");
    }

    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(clusterName)
            .forResource(resourceName).build();
    String zkPath = scope.getZkPath();

    if (overwrite) {
      ZKUtil.createOrReplace(_zkClient, zkPath, resourceConfig.getRecord(), true);
    } else {
      ZKUtil.createOrUpdate(_zkClient, zkPath, resourceConfig.getRecord(), true, true);
    }
  }

  /**
   * Get instance config for given resource in given cluster.
   *
   * @param clusterName
   * @param instanceName
   *
   * @return
   */
  public InstanceConfig getInstanceConfig(String clusterName, String instanceName) {
    if (!ZKUtil.isInstanceSetup(_zkClient, clusterName, instanceName, InstanceType.PARTICIPANT)) {
      throw new HelixException(
          "fail to get config. instance: " + instanceName + " is NOT setup in cluster: "
              + clusterName);
    }

    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT).forCluster(clusterName)
            .forParticipant(instanceName).build();
    ZNRecord record = getConfigZnRecord(scope);

    if (record == null) {
      LOG.warn("No config found at {}.", scope.getZkPath());
      return null;
    }

    return new InstanceConfig(record);
  }

  /**
   * Set config of the given instance config.
   * The current instance config will be replaced with the given instanceConfig.
   * WARNING: This is not thread-safe or concurrent updates safe.
   *
   * @param clusterName
   * @param instanceName
   * @param instanceConfig
   *
   * @return
   */
  public void setInstanceConfig(String clusterName, String instanceName,
      InstanceConfig instanceConfig) {
    updateInstanceConfig(clusterName, instanceName, instanceConfig, true);
  }

  /**
   * Update InstanceConfig of the given resource. The value of field in current config will be
   * replaced with the value of the same field in given config if it presents. If there is new field
   * in given config but not in current config, the field will be added into the current config..
   * The list fields and map fields will be replaced as a single entry.
   * The current Cluster config will be replaced with the given clusterConfig. WARNING: This is not
   * thread-safe or concurrent updates safe.
   * *
   *
   * @param clusterName
   * @param instanceName
   * @param instanceConfig
   *
   * @return
   */
  public void updateInstanceConfig(String clusterName, String instanceName,
      InstanceConfig instanceConfig) {
    updateInstanceConfig(clusterName, instanceName, instanceConfig, false);
  }

  private void updateInstanceConfig(String clusterName, String instanceName,
      InstanceConfig instanceConfig, boolean overwrite) {
    if (!ZKUtil.isClusterSetup(clusterName, _zkClient)) {
      throw new HelixException("fail to setup config. cluster: " + clusterName + " is NOT setup.");
    }

    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT).forCluster(clusterName)
            .forParticipant(instanceName).build();
    String zkPath = scope.getZkPath();

    if (!_zkClient.exists(zkPath)) {
      throw new HelixException(
          "updateInstanceConfig failed. Given InstanceConfig does not already exist. instance: "
              + instanceName);
    }

    if (overwrite) {
      ZKUtil.createOrReplace(_zkClient, zkPath, instanceConfig.getRecord(), true);
    } else {
      ZKUtil.createOrUpdate(_zkClient, zkPath, instanceConfig.getRecord(), true, true);
    }
  }

  /**
   * Closes ConfigAccessor: closes the stateful resources including the ZkClient.
   */
  public void close() {
    if (_zkClient != null && !_usesExternalZkClient) {
      _zkClient.close();
    }
  }

  public static class Builder extends GenericZkHelixApiBuilder<Builder> {
    public Builder() {
    }

    public ConfigAccessor build() {
      validate();
      return new ConfigAccessor(
          createZkClient(_realmMode, _realmAwareZkConnectionConfig, _realmAwareZkClientConfig,
              _zkAddress), false);
    }
  }
}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.util.StringTemplate;
import org.apache.log4j.Logger;

/**
 * Provides access to the persistent configuration of the cluster, the instances that live on it,
 * and the logical resources assigned to it.
 */
public class ConfigAccessor {
  private static Logger LOG = Logger.getLogger(ConfigAccessor.class);

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

  private final ZkClient zkClient;

  /**
   * Initialize an accessor with a Zookeeper client
   * @param zkClient
   */
  public ConfigAccessor(ZkClient zkClient) {
    this.zkClient = zkClient;
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
      LOG.error("fail to get configs. invalid config scope. scope: " + scope + ", keys: " + keys);
      return null;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, zkClient)) {
      throw new HelixException("fail to get configs. cluster " + clusterName + " is not setup yet");
    }

    Map<String, String> map = new HashMap<String, String>();

    ZNRecord record = zkClient.readData(scope.getZkPath(), true);
    if (record == null) {
      LOG.warn("No config found at " + scope.getZkPath());
      return null;
    }

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
        LOG.warn("No map-field found in " + record + " using mapKey: " + mapKey);
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
      LOG.error("fail to set config. invalid config scope. scope: " + scope);
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, zkClient)) {
      throw new HelixException("fail to set config. cluster: " + clusterName + " is NOT setup.");
    }

    if (scope.getType() == ConfigScopeProperty.PARTICIPANT) {
      if (!ZKUtil.isInstanceSetup(zkClient, scope.getClusterName(), scope.getParticipantName(),
          InstanceType.PARTICIPANT)) {
        throw new HelixException("fail to set config. instance: " + scope.getParticipantName()
            + " is NOT setup in cluster: " + clusterName);
      }
    }

    String zkPath = scope.getZkPath();
    String mapKey = scope.getMapKey();
    String id = zkPath.substring(zkPath.lastIndexOf('/') + 1);
    ZNRecord update = new ZNRecord(id);
    if (mapKey == null) {
      update.getSimpleFields().putAll(keyValueMap);
    } else {
      update.setMapField(mapKey, keyValueMap);
    }
    ZKUtil.createOrUpdate(zkClient, zkPath, update, true, true);
    return;
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
      LOG.error("fail to remove. invalid scope: " + scope + ", keys: " + keys);
      return;
    }

    String clusterName = scope.getClusterName();
    if (!ZKUtil.isClusterSetup(clusterName, zkClient)) {
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

    ZKUtil.subtract(zkClient, zkPath, update);
    return;
  }

  /**
   * Get list of config keys for a scope
   * @param scope
   * @return a list of configuration keys
   */
  public List<String> getKeys(HelixConfigScope scope) {
    if (scope == null || scope.getType() == null) {
      LOG.error("fail to getKeys. invalid config scope: " + scope);
      return null;
    }

    if (!ZKUtil.isClusterSetup(scope.getClusterName(), zkClient)) {
      LOG.error("fail to getKeys. cluster " + scope.getClusterName() + " is not setup yet");
      return Collections.emptyList();
    }

    String zkPath = scope.getZkPath();
    String mapKey = scope.getMapKey();
    List<String> retKeys = null;

    if (scope.isFullKey()) {
      ZNRecord record;
      try {
        record = zkClient.readData(zkPath);
      } catch (ZkNoNodeException e) {
        LOG.warn(zkPath + " no longer exists");
        return Collections.emptyList();
      }
      if (mapKey == null) {
        retKeys = new ArrayList<String>(record.getSimpleFields().keySet());
      } else {
        retKeys = new ArrayList<String>(record.getMapField(mapKey).keySet());
      }
    } else {
      if (scope.getType() == ConfigScopeProperty.PARTITION) {
        ZNRecord record = zkClient.readData(zkPath);
        retKeys = new ArrayList<String>(record.getMapFields().keySet());
      } else {
        retKeys = zkClient.getChildren(zkPath);
      }
    }

    if (retKeys != null) {
      Collections.sort(retKeys);
    }
    return retKeys;
  }
}

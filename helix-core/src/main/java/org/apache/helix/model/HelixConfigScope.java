package org.apache.helix.model;

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

import java.util.List;

import org.apache.helix.util.StringTemplate;

/**
 * config-scope that replaces @link ConfigScope
 */
public class HelixConfigScope {
  /**
   * Defines the various scopes of configs, and how they are represented on Zookeeper
   */
  public enum ConfigScopeProperty {
    CLUSTER(2, 0),
    PARTICIPANT(2, 0),
    RESOURCE(2, 0),
    PARTITION(2, 1),
    CONSTRAINT(2, 0);

    final int _zkPathArgNum;
    final int _mapKeyArgNum;

    private ConfigScopeProperty(int zkPathArgNum, int mapKeyArgNum) {
      _zkPathArgNum = zkPathArgNum;
      _mapKeyArgNum = mapKeyArgNum;
    }

    /**
     * Get the number of template arguments required to generate a full path
     * @return number of template arguments in the path
     */
    public int getZkPathArgNum() {
      return _zkPathArgNum;
    }

    /**
     * Get the number of arguments corresponding to a lookup key
     * @return number of map key arguments
     */
    public int getMapKeyArgNum() {
      return _mapKeyArgNum;
    }
  }

  /**
   * string templates to generate znode path
   */
  private static final StringTemplate template = new StringTemplate();
  static {
    // get the znode
    template.addEntry(ConfigScopeProperty.CLUSTER, 2,
        "/{clusterName}/CONFIGS/CLUSTER/{clusterName}");

    template.addEntry(ConfigScopeProperty.PARTICIPANT, 2,
        "/{clusterName}/CONFIGS/PARTICIPANT/{participantName}");
    template.addEntry(ConfigScopeProperty.RESOURCE, 2,
        "/{clusterName}/CONFIGS/RESOURCE/{resourceName}");
    template.addEntry(ConfigScopeProperty.PARTITION, 2,
        "/{clusterName}/CONFIGS/RESOURCE/{resourceName}");

    // get children
    template.addEntry(ConfigScopeProperty.CLUSTER, 1, "/{clusterName}/CONFIGS/CLUSTER");
    template.addEntry(ConfigScopeProperty.PARTICIPANT, 1, "/{clusterName}/CONFIGS/PARTICIPANT");
    template.addEntry(ConfigScopeProperty.RESOURCE, 1, "/{clusterName}/CONFIGS/RESOURCE");
  }

  final ConfigScopeProperty _type;
  final String _clusterName;

  /**
   * this is participantName if type is PARTICIPANT or null otherwise
   */
  final String _participantName;

  final String _resourceName;

  final String _zkPath;
  final String _mapKey;

  /**
   * use full-key to get config, use non-full-key to get config-keys
   */
  final boolean _isFullKey;

  /**
   * Initialize with a type of scope and unique identifiers
   * @param type the scope
   * @param zkPathKeys keys identifying a ZNode location
   * @param mapKey a key for an additional lookup within a ZNode
   */
  public HelixConfigScope(ConfigScopeProperty type, List<String> zkPathKeys, String mapKey) {

    if (zkPathKeys.size() != type.getZkPathArgNum()
        && zkPathKeys.size() != (type.getZkPathArgNum() - 1)) {
      throw new IllegalArgumentException(type + " requires " + type.getZkPathArgNum()
          + " arguments to get znode or " + (type.getZkPathArgNum() - 1)
          + " arguments to get children, but was: " + zkPathKeys);
    }

    if (type == ConfigScopeProperty.PARTITION) {
      _isFullKey = (zkPathKeys.size() == type.getZkPathArgNum()) && (mapKey != null);
    } else {
      _isFullKey = (zkPathKeys.size() == type.getZkPathArgNum());
    }

    _type = type;
    _clusterName = zkPathKeys.get(0);

    // init participantName
    if (type == ConfigScopeProperty.PARTICIPANT && _isFullKey) {
      _participantName = zkPathKeys.get(1);
    } else {
      _participantName = null;
    }

    // init resourceName
    if (type == ConfigScopeProperty.RESOURCE && _isFullKey) {
      _resourceName = zkPathKeys.get(1);
    } else {
      _resourceName = null;
    }

    _zkPath = template.instantiate(type, zkPathKeys.toArray(new String[0]));
    _mapKey = mapKey;
  }

  /**
   * Get the scope
   * @return the type of scope
   */
  public ConfigScopeProperty getType() {
    return _type;
  }

  /**
   * Get the cluster name
   * @return the name of the associated cluster
   */
  public String getClusterName() {
    return _clusterName;
  }

  /**
   * Get the participant name if it exists
   * @return the participant name if the type is PARTICIPANT, or null
   */
  public String getParticipantName() {
    return _participantName;
  }

  /**
   * Get the resource name if it exists
   * @return the resource name if the type is RESOURCE, or null
   */
  public String getResourceName() {
    return _resourceName;
  }

  /**
   * Get the path to the corresponding ZNode
   * @return a Zookeeper path
   */
  public String getZkPath() {
    return _zkPath;
  }

  /**
   * Get the lookup key within the ZNode if it exists
   * @return the lookup key, or null
   */
  public String getMapKey() {
    return _mapKey;
  }

  /**
   * Determine if the key gets a config key or the actual config
   * @return true if the key corresponds to a config, false if it corresponds to a config key
   */
  public boolean isFullKey() {
    return _isFullKey;
  }
}

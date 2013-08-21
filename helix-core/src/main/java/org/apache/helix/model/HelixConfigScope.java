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

    public int getZkPathArgNum() {
      return _zkPathArgNum;
    }

    public int getMapKeyArgNum() {
      return _mapKeyArgNum;
    }
  }

  // string templates to generate znode path
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

  final String _zkPath;
  final String _mapKey;

  /**
   * use full-key to get config, use non-full-key to get config-keys
   */
  final boolean _isFullKey;

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

    _zkPath = template.instantiate(type, zkPathKeys.toArray(new String[0]));
    _mapKey = mapKey;
  }

  public ConfigScopeProperty getType() {
    return _type;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public String getParticipantName() {
    return _participantName;
  }

  public String getZkPath() {
    return _zkPath;
  }

  public String getMapKey() {
    return _mapKey;
  }

  public boolean isFullKey() {
    return _isFullKey;
  }
}

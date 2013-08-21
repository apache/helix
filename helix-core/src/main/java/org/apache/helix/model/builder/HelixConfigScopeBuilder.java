package org.apache.helix.model.builder;

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

import java.util.Arrays;

import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;

/**
 * config-scope builder that replaces @link ConfigScopeBuilder
 */
public class HelixConfigScopeBuilder {

  private final ConfigScopeProperty _type;
  private String _clusterName;
  private String _participantName;
  private String _resourceName;
  private String _partitionName;

  public HelixConfigScopeBuilder(ConfigScopeProperty type, String... keys) {
    int argNum = type.getZkPathArgNum() + type.getMapKeyArgNum();
    if (keys == null || (keys.length != argNum && keys.length != argNum - 1)) {
      throw new IllegalArgumentException("invalid keys. type: " + type + ", keys: "
          + Arrays.asList(keys));
    }

    _type = type;
    _clusterName = keys[0];

    switch (type) {
    case CLUSTER:
      break;
    case PARTICIPANT:
      if (keys.length > 1) {
        _participantName = keys[1];
      }
      break;
    case RESOURCE:
      if (keys.length > 1) {
        _resourceName = keys[1];
      }
      break;
    case PARTITION:
      _resourceName = keys[1];
      if (keys.length > 2) {
        _partitionName = keys[2];
      }
      break;
    default:
      break;
    }
  }

  public HelixConfigScopeBuilder(ConfigScopeProperty type) {
    _type = type;
  }

  public HelixConfigScopeBuilder forCluster(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public HelixConfigScopeBuilder forParticipant(String participantName) {
    _participantName = participantName;
    return this;
  }

  public HelixConfigScopeBuilder forResource(String resourceName) {
    _resourceName = resourceName;
    return this;
  }

  public HelixConfigScopeBuilder forPartition(String partitionName) {
    _partitionName = partitionName;
    return this;
  }

  public HelixConfigScope build() {
    HelixConfigScope scope = null;
    switch (_type) {
    case CLUSTER:
      scope = new HelixConfigScope(_type, Arrays.asList(_clusterName, _clusterName), null);
      break;
    case PARTICIPANT:
      if (_participantName == null) {
        scope = new HelixConfigScope(_type, Arrays.asList(_clusterName), null);
      } else {
        scope = new HelixConfigScope(_type, Arrays.asList(_clusterName, _participantName), null);
      }
      break;
    case RESOURCE:
      if (_resourceName == null) {
        scope = new HelixConfigScope(_type, Arrays.asList(_clusterName), null);
      } else {
        scope = new HelixConfigScope(_type, Arrays.asList(_clusterName, _resourceName), null);
      }
      break;
    case PARTITION:
      if (_partitionName == null) {
        scope = new HelixConfigScope(_type, Arrays.asList(_clusterName, _resourceName), null);
      } else {
        scope =
            new HelixConfigScope(_type, Arrays.asList(_clusterName, _resourceName), _partitionName);
      }
      break;
    default:
      break;
    }
    return scope;
  }
}

package org.apache.helix.rest.common.datamodel;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;

/* This Snapshot can extend Snapshot from common/core module
 * once there is more generic snapshot.
 * An Snapshot object should contain all the Helix related info that an implementation of
 * OperationAbstractClass would need.
 */

// TODO: Future: Support hierarchical Snapshot type for other services besides cluster MaintenanceService.

public class RestSnapShot {
  private final Map<PropertyKey, HelixProperty> _propertyCache;
  private final Map<PropertyKey, List<String>> _childNodesCache;
  private Set<PropertyType> _propertyTypes;
  private String _clusterName;

  public RestSnapShot(String clusterName) {
    _propertyCache = new HashMap<>();
    _childNodesCache = new HashMap<>();
    _propertyTypes = new HashSet<>();
    _clusterName = clusterName;
  }

  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    if (_propertyCache.containsKey(key)) {
      return (T) _propertyCache.get(key);
    }
    return null;
  }

  public List<String> getChildNames(PropertyKey key) {
    if (_childNodesCache.containsKey(key)) {
      return _childNodesCache.get(key);
    }
    return null;
  }

  public void updateProperty(PropertyKey key,  HelixProperty property){
    _propertyCache.put(key, property);
  }

  public void updateChildNames(PropertyKey key,  List<String> children){
    _childNodesCache.put(key, children);
  }

  public void addPropertyType(PropertyType propertyType) {
    _propertyTypes.add(propertyType);
  }

  public boolean containsProperty(PropertyType propertyType) {
    return _propertyTypes.contains(propertyType);
  }

  public String getClusterName() {
    return _clusterName;
  }

}
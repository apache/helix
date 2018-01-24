package org.apache.helix.view.aggregator;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.PropertyType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.model.ClusterConfig;

/**
 * This class takes an existing list of source cluster configs and generate lists of source
 * cluster configs to add / delete / modify, and tell caller if ViewClusterRefreshTimer should
 * be reset or not
 */
public class SourceClusterConfigChangeAction {
  private ClusterConfig _oldConfig;
  private ClusterConfig _newConfig;

  private boolean _shouldResetTimer;
  private List<ViewClusterSourceConfig> _toAdd;
  private List<ViewClusterSourceConfig> _toDelete;

  public SourceClusterConfigChangeAction(ClusterConfig oldConfig, ClusterConfig newConfig) {
    _oldConfig = oldConfig;
    _newConfig = newConfig;
    _shouldResetTimer = false;
    _toAdd = new ArrayList<>();
    _toDelete = new ArrayList<>();
    validate();
  }

  private void validate() {
    StringBuilder allErrs = new StringBuilder();
    if (_newConfig == null) {
      allErrs.append("New config is null;");
    }
    if (_oldConfig != null && !_oldConfig.isViewCluster()) {
      allErrs.append("Old config is not view cluster config;");
    }
    if (_newConfig != null && !_newConfig.isViewCluster()) {
      allErrs.append("New config is not view cluster config;");
    }
    if (allErrs.length() > 0) {
      throw new IllegalArgumentException(allErrs.toString());
    }
  }

  /**
   * Compute actions and generate toAdd, toDelete toModify, and refreshPeriodChanged.
   */
  public void computeAction() {
    _shouldResetTimer = _oldConfig == null || _oldConfig.getViewClusterRefershPeriod() != _newConfig
        .getViewClusterRefershPeriod();

    Map<String, ViewClusterSourceConfig> oldConfigMap = new HashMap<>();
    Map<String, ViewClusterSourceConfig> currentConfigMap = new HashMap<>();

    if (_oldConfig != null) {
      for (ViewClusterSourceConfig oldConfig : _oldConfig.getViewClusterSourceConfigs()) {
        oldConfigMap
            .put(generateConfigMapKey(oldConfig.getName(), oldConfig.getZkAddress()), oldConfig);
      }
    }

    for (ViewClusterSourceConfig currentConfig : _newConfig.getViewClusterSourceConfigs()) {
      currentConfigMap
          .put(generateConfigMapKey(currentConfig.getName(), currentConfig.getZkAddress()),
              currentConfig);
    }

    // Configs whose properties-to-aggregate got modified should have its data provider recreated
    for (Map.Entry<String, ViewClusterSourceConfig> entry : currentConfigMap.entrySet()) {
      if (oldConfigMap.containsKey(entry.getKey())) {
        Set<PropertyType> oldPropertySet =
            new HashSet<>(oldConfigMap.get(entry.getKey()).getProperties());
        Set<PropertyType> currentPropertySet = new HashSet<>(entry.getValue().getProperties());
        if (!oldPropertySet.equals(currentPropertySet)) {
          _toAdd.add(new ViewClusterSourceConfig(entry.getValue()));
          _toDelete.add(new ViewClusterSourceConfig(oldConfigMap.get(entry.getKey())));
        }
      }
    }

    // Generate toDelete. Use deep copy to avoid race conditions
    Set<String> removedKeys = new HashSet<>(oldConfigMap.keySet());
    removedKeys.removeAll(currentConfigMap.keySet());
    for (String removedKey : removedKeys) {
      _toDelete.add(new ViewClusterSourceConfig(oldConfigMap.get(removedKey)));
    }

    // Generate toAdd. Use deep copy to avoid race conditions
    Set<String> addedKeys = new HashSet<>(currentConfigMap.keySet());
    addedKeys.removeAll(oldConfigMap.keySet());
    for (String addedKey : addedKeys) {
      _toAdd.add(new ViewClusterSourceConfig(currentConfigMap.get(addedKey)));
    }
  }

  public List<ViewClusterSourceConfig> getConfigsToAdd() {
    return _toAdd;
  }

  public List<ViewClusterSourceConfig> getConfigsToDelete() {
    return _toDelete;
  }

  public boolean shouldResetTimer() {
    return _shouldResetTimer;
  }

  public long getCurrentRefreshPeriodMs() {
    return _newConfig.getViewClusterRefershPeriod() * 1000;
  }

  private String generateConfigMapKey(String clusterName, String zkAddr) {
    return String.format("%s%s", clusterName, zkAddr);
  }
}

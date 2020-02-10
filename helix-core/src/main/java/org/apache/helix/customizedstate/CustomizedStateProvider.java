package org.apache.helix.customizedstate;

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
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.CustomizedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for Helix customers to operate on customized state
 */
public class CustomizedStateProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CustomizedStateProvider.class);
  private final HelixManager _helixManager;
  String _instanceName;

  public CustomizedStateProvider(HelixManager helixManager, String instanceName) {
    _helixManager = helixManager;
    _instanceName = instanceName;
  }

  /**
   * Update the customized state based on the resource name and partition name
   */
  public synchronized void updateCustomizedState(String customizedStateName, String resourceName,
      String partitionName, Map<String, String> customizedState) {
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    PropertyKey propertyKey =
        keyBuilder.customizedState(_instanceName, customizedStateName, resourceName);
    ZNRecord record = new ZNRecord(resourceName);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    mapFields.put(partitionName, customizedState);
    record.setMapFields(mapFields);
    if (!accessor.updateProperty(propertyKey, new CustomizedState(record))) {
        throw new HelixException(String.format(
            "Failed to persist customized state %s to zk for instance %s, resource %s",
            customizedStateName, _instanceName, record.getId()));
      }
    }

  /**
   * Get the customized state for a specified resource
   */
  public CustomizedState getCustomizedState(String customizedStateName, String resourceName) {
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    return (CustomizedState) accessor
        .getProperty(keyBuilder.customizedState(_instanceName, customizedStateName, resourceName));
  }

  /**
   * Get the customized state for a specified resource and a specified partition
   */
  public Map<String, String> getPerPartitionCustomizedState(String customizedStateName,
      String resourceName, String partitionName) {
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Map<String, Map<String, String>> mapView = accessor
        .getProperty(keyBuilder.customizedState(_instanceName, customizedStateName, resourceName))
        .getRecord().getMapFields();
    return mapView.get(partitionName);
  }
}

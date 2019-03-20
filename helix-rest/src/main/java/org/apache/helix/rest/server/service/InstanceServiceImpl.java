package org.apache.helix.rest.server.service;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.util.InstanceValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InstanceServiceImpl implements InstanceService {
  private static final Logger _logger = LoggerFactory.getLogger(InstanceServiceImpl.class);

  private final HelixDataAccessor _dataAccessor;
  private final ConfigAccessor _configAccessor;

  public InstanceServiceImpl(HelixDataAccessor dataAccessor, ConfigAccessor configAccessor) {
    _dataAccessor = dataAccessor;
    _configAccessor = configAccessor;
  }

  @Override
  public Map<String, Boolean> getInstanceStoppableCheck(String clusterId, String instanceName) {
    Map<String, Boolean> healthStatus = new HashMap<>();
    healthStatus.put(HealthStatus.HAS_VALID_CONFIG.name(), InstanceValidationUtil.hasValidConfig(_dataAccessor, clusterId, instanceName));
    if (!healthStatus.get(HealthStatus.HAS_VALID_CONFIG.name())) {
      _logger.error("The instance {} doesn't have valid configuration", instanceName);
      return healthStatus;
    }

    // Any exceptions occurred below due to invalid instance config shouldn't happen
    healthStatus.put(HealthStatus.IS_ENABLED.name(), InstanceValidationUtil.isEnabled(_dataAccessor, _configAccessor, clusterId, instanceName));
    healthStatus.put(HealthStatus.IS_ALIVE.name(), InstanceValidationUtil.isAlive(_dataAccessor, clusterId, instanceName));
    healthStatus.put(HealthStatus.HAS_RESOURCE_ASSIGNED.name(), InstanceValidationUtil.hasResourceAssigned(_dataAccessor, clusterId, instanceName));
    healthStatus.put(HealthStatus.HAS_DISABLED_PARTITIONS.name(), InstanceValidationUtil.hasDisabledPartitions(_dataAccessor, clusterId, instanceName));
    healthStatus.put(HealthStatus.HAS_ERROR_PARTITIONS.name(), InstanceValidationUtil.hasErrorPartitions(_dataAccessor, clusterId, instanceName));

    try {
      boolean isStable = InstanceValidationUtil.isInstanceStable(_dataAccessor, instanceName);
      healthStatus.put(HealthStatus.IS_STABLE.name(), isStable);
    } catch (HelixException e) {
      _logger.error("Failed to check instance is stable, message: {}", e.getMessage());
      // TODO action on the stable check exception
    }

    return healthStatus;
  }

  private enum HealthStatus {
    IS_ALIVE,
    IS_ENABLED,
    HAS_RESOURCE_ASSIGNED,
    HAS_DISABLED_PARTITIONS,
    HAS_VALID_CONFIG,
    HAS_ERROR_PARTITIONS,
    IS_STABLE
  }
}

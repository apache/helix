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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.rest.clusterMaintenanceService.HealthCheck;
import org.apache.helix.rest.clusterMaintenanceService.MaintenanceManagementService;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.apache.helix.rest.server.json.instance.InstanceInfo;
import org.apache.helix.rest.server.json.instance.StoppableCheck;


public class InstanceServiceImpl implements InstanceService {
  private MaintenanceManagementService _maintenanceManagementService;

  @Deprecated
  public InstanceServiceImpl(ZKHelixDataAccessor dataAccessor, ConfigAccessor configAccessor) {
    this(dataAccessor, configAccessor, false);
  }

  @Deprecated
  public InstanceServiceImpl(ZKHelixDataAccessor dataAccessor, ConfigAccessor configAccessor,
      boolean skipZKRead) {
    this(dataAccessor, configAccessor, skipZKRead, HelixRestNamespace.DEFAULT_NAMESPACE_NAME);
  }

  public InstanceServiceImpl(ZKHelixDataAccessor dataAccessor, ConfigAccessor configAccessor,
      boolean skipZKRead, String namespace) {
    this(dataAccessor, configAccessor, skipZKRead, false, namespace);
  }

  // TODO: too many params, convert to builder pattern
  public InstanceServiceImpl(ZKHelixDataAccessor dataAccessor, ConfigAccessor configAccessor,
      boolean skipZKRead, boolean continueOnFailures, String namespace) {
    _maintenanceManagementService =
        new MaintenanceManagementService(dataAccessor, configAccessor, skipZKRead,
            continueOnFailures, namespace);
  }


  @Override
  public InstanceInfo getInstanceInfo(String clusterId, String instanceName,
      List<HealthCheck> healthChecks) {
    return _maintenanceManagementService.getInstanceHealthInfo(clusterId, instanceName, healthChecks);
  }

  /**
   * {@inheritDoc}
   * Single instance stoppable check implementation is a special case of
   * {@link #batchGetInstancesStoppableChecks(String, List, String)}
   * <p>
   * Step 1: Perform instance level Helix own health checks
   * Step 2: Perform instance level client side health checks
   * Step 3: Perform partition level (all partitions on the instance) client side health checks
   * <p>
   * Note: if the check fails at one step, the rest steps won't be executed because the instance
   * cannot be stopped
   */
  @Override
  public StoppableCheck getInstanceStoppableCheck(String clusterId, String instanceName,
      String jsonContent) throws IOException {
    return batchGetInstancesStoppableChecks(clusterId, ImmutableList.of(instanceName), jsonContent)
        .get(instanceName);
  }

  @Override
  public Map<String, StoppableCheck> batchGetInstancesStoppableChecks(String clusterId,
      List<String> instances, String jsonContent) throws IOException {
    return _maintenanceManagementService
        .batchGetInstancesStoppableChecks(clusterId, instances, jsonContent);
  }

}

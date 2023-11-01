package org.apache.helix.rest.clusterMaintenanceService;

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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.rest.client.CustomRestClient;
import org.apache.helix.rest.server.json.instance.StoppableCheck;


public class MaintenanceManagementServiceBuilder {
  private ConfigAccessor _configAccessor;
  private boolean _skipZKRead;
  private String _namespace;
  private ZKHelixDataAccessor _dataAccessor;
  private CustomRestClient _customRestClient;
  private boolean _continueOnFailure;
  private Set<StoppableCheck.Category> _skipHealthCheckCategories = Collections.emptySet();
  private List<HealthCheck> _stoppableHealthCheckList = Collections.emptyList();

  public ConfigAccessor getConfigAccessor() {
    return _configAccessor;
  }

  public boolean isSkipZKRead() {
    return _skipZKRead;
  }

  public String getNamespace() {
    return _namespace;
  }

  public ZKHelixDataAccessor getDataAccessor() {
    return _dataAccessor;
  }

  public CustomRestClient getCustomRestClient() {
    return _customRestClient;
  }

  public boolean isContinueOnFailure() {
    return _continueOnFailure;
  }

  public Set<StoppableCheck.Category> getSkipHealthCheckCategories() {
    return _skipHealthCheckCategories;
  }

  public List<HealthCheck> getStoppableHealthCheckList() {
    return _stoppableHealthCheckList;
  }

  public MaintenanceManagementServiceBuilder setConfigAccessor(ConfigAccessor configAccessor) {
    _configAccessor = configAccessor;
    return this;
  }

  public MaintenanceManagementServiceBuilder setSkipZKRead(boolean skipZKRead) {
    _skipZKRead = skipZKRead;
    return this;
  }

  public MaintenanceManagementServiceBuilder setNamespace(String namespace) {
    _namespace = namespace;
    return this;
  }

  public MaintenanceManagementServiceBuilder setDataAccessor(
      ZKHelixDataAccessor dataAccessor) {
    _dataAccessor = dataAccessor;
    return this;
  }

  public MaintenanceManagementServiceBuilder setCustomRestClient(
      CustomRestClient customRestClient) {
    _customRestClient = customRestClient;
    return this;
  }

  public MaintenanceManagementServiceBuilder setContinueOnFailure(boolean continueOnFailure) {
    _continueOnFailure = continueOnFailure;
    return this;
  }

  public MaintenanceManagementServiceBuilder setSkipHealthCheckCategories(
      Set<StoppableCheck.Category> skipHealthCheckCategories) {
    _skipHealthCheckCategories = skipHealthCheckCategories;
    return this;
  }

  public MaintenanceManagementServiceBuilder setStoppableHealthCheckList(
      List<HealthCheck> stoppableHealthCheckList) {
    _stoppableHealthCheckList = stoppableHealthCheckList;
    return this;
  }

  public MaintenanceManagementService createMaintenanceManagementService() {
    if (_configAccessor == null || _namespace == null || _dataAccessor == null
        || _customRestClient == null) {
      throw new IllegalArgumentException(
          "One or more of following mandatory arguments are not set: '_configAccessor', "
              + "'_namespace', '_dataAccessor', '_customRestClient'.");
    }
    return new MaintenanceManagementService(this);
  }
}
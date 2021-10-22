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

import java.util.Collection;
import java.util.Map;


public interface OperationAbstractClass {

  // operation check
  MaintenanceManagementInstanceInfo operationCheckForTakeSingleInstance(String instanceName,
       Map<String, String> operationConfig);

  MaintenanceManagementInstanceInfo operationCheckForFreeSingleInstance(String instanceName,
      Map<String, String> operationConfig);

  Map<String, MaintenanceManagementInstanceInfo> operationCheckForTakeInstances(
      Collection<String> instances, Map<String, String> operationConfig);

  Map<String, MaintenanceManagementInstanceInfo> operationCheckForFreeInstances(
      Collection<String> instances, Map<String, String> operationConfig);

  // operation execute
  MaintenanceManagementInstanceInfo operationExecForTakeSingleInstance(String instanceName,
      Map<String, String> operationConfig);

  MaintenanceManagementInstanceInfo operationExecForFreeSingleInstance(String instanceName,
      Map<String, String> operationConfig);

  Map<String, MaintenanceManagementInstanceInfo> operationExecForTakeInstances(
      Collection<String> instances, Map<String, String> operationConfig);

  Map<String, MaintenanceManagementInstanceInfo> operationExecForFreeInstances(
      Collection<String> instances, Map<String, String> operationConfig);
}

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

/* This Snapshot can extend Snapshot from common/core module
 * once there is more generic snapshot.
 */
public class RestSnapShot {
  /* An Snapshot object should contain all the Helix related info that an implementation of
   * OperationAbstractClass would need.
   */

  // TODO: Define a Enum class for all Helix info types like ExternalView, InstanceConfig etc. An
  // implementation of OperationAbstractClass will need to define what are the types needed.

  // TODO: Support hierarchical Snapshot type for other services besides cluster MaintenanceService.
}
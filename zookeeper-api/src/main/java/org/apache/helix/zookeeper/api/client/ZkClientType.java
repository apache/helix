package org.apache.helix.zookeeper.api.client;

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

public enum ZkClientType {
  /**
   * If a Helix API is created with the DEDICATED type, it supports ephemeral node
   * creation, callback functionality, and session management. But note that this is more
   * resource-heavy since it creates a dedicated ZK connection so should be used sparingly only
   * when the aforementioned features are needed.
   *
   * Valid on SINGLE_REALM only.
   */
  DEDICATED,

  /**
   * If a Helix API is created with the SHARED type, it only supports CRUD
   * functionalities. This will be the default mode of creation.
   *
   * Valid on SINGLE_REALM only.
   */
  SHARED,

  /**
   * Uses FederatedZkClient (applicable on multi-realm mode only) that queries Metadata Store
   * Directory Service for routing data.
   *
   * Valid on MULTI_REALM only.
   */
  FEDERATED
}

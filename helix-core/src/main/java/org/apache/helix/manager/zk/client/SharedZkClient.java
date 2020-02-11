package org.apache.helix.manager.zk.client;

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

/**
 * Deprecated; use SharedZkClient in zookeeper-api instead.
 */
@Deprecated
class SharedZkClient extends org.apache.helix.zookeeper.impl.client.SharedZkClient {
  /**
   * Construct a shared RealmAwareZkClient that uses a shared ZkConnection.
   *  @param connectionManager     The manager of the shared ZkConnection.
   * @param clientConfig          ZkClientConfig details to create the shared RealmAwareZkClient.
   * @param callback              Clean up logic when the shared RealmAwareZkClient is closed.
   */
  protected SharedZkClient(ZkConnectionManager connectionManager, ZkClientConfig clientConfig,
      org.apache.helix.zookeeper.impl.client.SharedZkClient.OnCloseCallback callback) {
    super(connectionManager, clientConfig, callback);
  }
}

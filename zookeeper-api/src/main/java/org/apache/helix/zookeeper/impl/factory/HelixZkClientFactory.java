package org.apache.helix.zookeeper.impl.factory;

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

import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.factory.RealmAwareZkClientFactory;
import org.apache.helix.zookeeper.exception.ZkClientException;
import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.helix.zookeeper.zkclient.ZkConnection;


/**
 * Abstract class of the ZkClient factory.
 */
public abstract class HelixZkClientFactory implements RealmAwareZkClientFactory {

  /**
   * Build a ZkClient using specified connection config and client config
   *
   * @param connectionConfig
   * @param clientConfig
   * @return HelixZkClient
   */
  @Deprecated
  public abstract HelixZkClient buildZkClient(HelixZkClient.ZkConnectionConfig connectionConfig,
      HelixZkClient.ZkClientConfig clientConfig);

  /**
   * Build a ZkClient using specified connection config and default client config
   *
   * @param connectionConfig
   * @return HelixZkClient
   */
  @Deprecated
  public HelixZkClient buildZkClient(HelixZkClient.ZkConnectionConfig connectionConfig) {
    return buildZkClient(connectionConfig, new HelixZkClient.ZkClientConfig());
  }

  /**
   * Construct a new ZkConnection instance based on connection configuration.
   * Note that the connection is not really made until someone calls zkConnection.connect().
   * @param connectionConfig
   * @return
   */
  protected IZkConnection createZkConnection(HelixZkClient.ZkConnectionConfig connectionConfig) {
    if (connectionConfig.getZkServers() == null) {
      throw new ZkClientException(
          "Failed to build ZkClient since no connection or ZK server address is specified.");
    } else {
      return new ZkConnection(connectionConfig.getZkServers(),
          connectionConfig.getSessionTimeout());
    }
  }
}

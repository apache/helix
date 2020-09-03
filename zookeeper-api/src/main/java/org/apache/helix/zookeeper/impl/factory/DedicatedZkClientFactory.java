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

import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.impl.client.DedicatedZkClient;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Singleton factory that build dedicated clients using the raw ZkClient.
 */
public class DedicatedZkClientFactory extends HelixZkClientFactory {

  protected DedicatedZkClientFactory() {
  }

  @Override
  public RealmAwareZkClient buildZkClient(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig)
      throws InvalidRoutingDataException {
    return new DedicatedZkClient(connectionConfig, clientConfig);
  }

  private static class SingletonHelper {
    private static final DedicatedZkClientFactory INSTANCE = new DedicatedZkClientFactory();
  }

  public static DedicatedZkClientFactory getInstance() {
    return SingletonHelper.INSTANCE;
  }

  /**
   * Build a Dedicated ZkClient based on connection config and client config.
   * @param connectionConfig
   * @param clientConfig
   * @return
   */
  @Override
  public HelixZkClient buildZkClient(HelixZkClient.ZkConnectionConfig connectionConfig,
      HelixZkClient.ZkClientConfig clientConfig) {
    return new ZkClient(createZkConnection(connectionConfig),
        (int) clientConfig.getConnectInitTimeout(), clientConfig.getOperationRetryTimeout(),
        clientConfig.getZkSerializer(), clientConfig.getMonitorType(), clientConfig.getMonitorKey(),
        clientConfig.getMonitorInstanceName(), clientConfig.isMonitorRootPathOnly());
  }
}

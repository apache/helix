package org.apache.helix.manager.zk;

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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixException;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.api.client.ZkClientType;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;


/**
 * GenericBaseDataAccessorBuilder serves as the abstract parent class for Builders used by
 * BaseDataAccessor APIs that create ZK connections. By having this class, we promote code-reuse.
 * @param <B>
 */
public class GenericBaseDataAccessorBuilder<B extends GenericBaseDataAccessorBuilder<B>> extends GenericZkHelixApiBuilder<B> {
  /** ZK-based BaseDataAccessor-specific parameter **/
  private ZkClientType _zkClientType;

  /**
   * Sets the ZkClientType.
   * If this is set to either DEDICATED or SHARED, this accessor will be created on
   * single-realm mode.
   * If this is set to FEDERATED, multi-realm mode will be used.
   * @param zkClientType
   * @return
   */
  public B setZkClientType(ZkBaseDataAccessor.ZkClientType zkClientType) {
    return setZkClientType(Enum.valueOf(ZkClientType.class, zkClientType.name()));
  }

  public B setZkClientType(ZkClientType zkClientType) {
    _zkClientType = zkClientType;
    return self();
  }

  /**
   * Validates the given parameters before building an instance of ZkBaseDataAccessor.
   */
  @Override
  protected void validate() {
    super.validate();
    validateZkClientType(_zkClientType, _realmMode);
  }

  /**
   * This method contains construction logic for ZK-based BaseDataAccessor
   * implementations.
   * It uses an implementation of GenericBaseDataAccessorBuilder to construct the right
   * RealmAwareZkClient based on a host of configs provided in the Builder.
   * @return RealmAwareZkClient
   */
  @Override
  protected RealmAwareZkClient createZkClient(RealmAwareZkClient.RealmMode realmMode,
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig, String zkAddress) {
    RealmAwareZkClient zkClient;
    switch (realmMode) {
      case MULTI_REALM:
        try {
          if (_zkClientType == ZkClientType.DEDICATED) {
            // Use a realm-aware dedicated zk client
            zkClient = DedicatedZkClientFactory.getInstance()
                .buildZkClient(connectionConfig, clientConfig);
          } else if (_zkClientType == ZkClientType.SHARED) {
            // Use a realm-aware shared zk client
            zkClient =
                SharedZkClientFactory.getInstance().buildZkClient(connectionConfig, clientConfig);
          } else {
            zkClient = new FederatedZkClient(connectionConfig, clientConfig);
          }
        } catch (IOException | InvalidRoutingDataException | IllegalStateException e) {
          throw new HelixException("Not able to connect on multi-realm mode.", e);
        }
        break;

      case SINGLE_REALM:
        if (_zkClientType == ZkClientType.DEDICATED) {
          // If DEDICATED, then we use a dedicated HelixZkClient because we must support ephemeral
          // operations
          zkClient = DedicatedZkClientFactory.getInstance()
              .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
                  clientConfig.createHelixZkClientConfig());
        } else {
          // if SHARED: Use a SharedZkClient because ZkBaseDataAccessor does not need to
          // do ephemeral operations.
          zkClient = SharedZkClientFactory.getInstance()
              .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
                  clientConfig.createHelixZkClientConfig());
          zkClient
              .waitUntilConnected(HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
        }
        break;
      default:
        throw new HelixException("Invalid RealmMode given: " + realmMode);
    }
    return zkClient;
  }

  /**
   * Validate ZkClientType based on RealmMode.
   * @param zkClientType
   * @param realmMode
   */
  private void validateZkClientType(ZkClientType zkClientType,
      RealmAwareZkClient.RealmMode realmMode) {
    boolean isZkClientTypeSet = zkClientType != null;
    // If ZkClientType is set, RealmMode must either be single-realm or not set.
    if (isZkClientTypeSet && realmMode == RealmAwareZkClient.RealmMode.MULTI_REALM) {
      throw new HelixException("ZkClientType cannot be set on multi-realm mode!");
    }
    // If ZkClientType is not set and realmMode is single-realm, default to SHARED
    if (!isZkClientTypeSet && realmMode == RealmAwareZkClient.RealmMode.SINGLE_REALM) {
      zkClientType = ZkClientType.SHARED;
    }
    if (realmMode == RealmAwareZkClient.RealmMode.SINGLE_REALM
        && zkClientType == ZkClientType.FEDERATED) {
      throw new HelixException("FederatedZkClient cannot be set on single-realm mode!");
    }
  }
}

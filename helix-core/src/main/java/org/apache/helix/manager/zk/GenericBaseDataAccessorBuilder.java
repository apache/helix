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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
          zkClient = new FederatedZkClient(connectionConfig, clientConfig);
        } catch (InvalidRoutingDataException e) {
          throw new HelixException("Not able to connect on multi-realm mode.", e);
        }
        break;
      case SINGLE_REALM:
        HelixZkClient.ZkConnectionConfig helixZkConnectionConfig =
            new HelixZkClient.ZkConnectionConfig(zkAddress)
                .setSessionTimeout(connectionConfig.getSessionTimeout());
        if (_zkClientType == ZkClientType.DEDICATED) {
          // If DEDICATED, then we use a dedicated HelixZkClient because we must support ephemeral
          // operations
          zkClient = DedicatedZkClientFactory.getInstance()
              .buildZkClient(helixZkConnectionConfig, clientConfig.createHelixZkClientConfig());
        } else {
          // if SHARED or null: Use a SharedZkClient because ZkBaseDataAccessor does not need to
          // do ephemeral operations.
          zkClient = SharedZkClientFactory.getInstance()
              .buildZkClient(helixZkConnectionConfig, clientConfig.createHelixZkClientConfig());
        }
        zkClient
            .waitUntilConnected(HelixZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
        break;
      default:
        throw new HelixException("Invalid RealmMode given: " + realmMode);
    }
    return zkClient;
  }

  /**
   * Validate ZkClientType based on RealmMode.
   * If ZkClientType is DEDICATED or SHARED, the realm mode must be SINGLE-REALM.
   * If ZkClientType is FEDERATED, the realm mode must be MULTI-REALM.
   * @param zkClientType
   * @param realmMode
   */
  private void validateZkClientType(ZkClientType zkClientType,
      RealmAwareZkClient.RealmMode realmMode) {
    if (realmMode == null) {
      // NOTE: GenericZkHelixApiBuilder::validate() is and must be called before this function, so
      // we could assume that realmMode will not be null. If it is, we throw an exception.
      throw new HelixException(
          "GenericBaseDataAccessorBuilder: Cannot validate ZkClient type! RealmMode is null!");
    }
    // If ZkClientType is DEDICATED or SHARED, the realm mode cannot be multi-realm.
    // If ZkClientType is FEDERATED, the realm mode cannot be single-realm.
    if (((zkClientType == ZkClientType.DEDICATED || zkClientType == ZkClientType.SHARED)
        && realmMode == RealmAwareZkClient.RealmMode.MULTI_REALM) || (
        zkClientType == ZkClientType.FEDERATED
            && realmMode == RealmAwareZkClient.RealmMode.SINGLE_REALM)) {
      throw new HelixException(
          "Invalid combination of ZkClientType and RealmMode: ZkClientType is " + zkClientType
              .name() + " and realmMode is " + realmMode.name());
    }
  }
}

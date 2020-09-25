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

import org.apache.helix.HelixException;
import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GenericZkHelixApiBuilder serves as the abstract parent class for Builders used by Helix Java APIs
 * that create ZK connections. By having this class, we reduce duplicate code as much as possible.
 * @param <B>
 */
public abstract class GenericZkHelixApiBuilder<B extends GenericZkHelixApiBuilder<B>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(GenericZkHelixApiBuilder.class.getName());

  protected String _zkAddress;
  protected RealmAwareZkClient.RealmMode _realmMode;
  protected RealmAwareZkClient.RealmAwareZkConnectionConfig _realmAwareZkConnectionConfig;
  protected RealmAwareZkClient.RealmAwareZkClientConfig _realmAwareZkClientConfig;

  /**
   * Note: If you set the ZK address explicitly, this setting will take priority over the ZK path
   * sharding key set in RealmAwareZkConnectionConfig.
   * If this field is null, and the realm mode is single-realm, then it will try to look up the
   * ZK address based on the ZK path sharding key.
   * @param zkAddress
   * @return
   */
  public B setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return self();
  }

  public B setRealmMode(RealmAwareZkClient.RealmMode realmMode) {
    _realmMode = realmMode;
    return self();
  }

  public B setRealmAwareZkConnectionConfig(
      RealmAwareZkClient.RealmAwareZkConnectionConfig realmAwareZkConnectionConfig) {
    _realmAwareZkConnectionConfig = realmAwareZkConnectionConfig;
    return self();
  }

  public B setRealmAwareZkClientConfig(
      RealmAwareZkClient.RealmAwareZkClientConfig realmAwareZkClientConfig) {
    _realmAwareZkClientConfig = realmAwareZkClientConfig;
    return self();
  }

  /**
   * Validates the given Builder parameters using a generic validation logic.
   *
   * This validation function checks whether realm mode has been set correctly, and
   * resolves them if not set at all.
   *
   * If realm mode is null, we look at zkAddress field or RealmAwareZkClient's connection config to
   * see if we could deduce the Zk address based on the MetadataStoreRoutingData.
   *
   * Note: If you set the ZK address explicitly, this setting will take priority over the ZK path
   * sharding key set in RealmAwareZkConnectionConfig.
   * If this field is null, and the realm mode is single-realm, then it will try to look up the
   * ZK address based on the ZK path sharding key.
   */
  protected void validate() {
    initializeConfigsIfNull();

    // Resolve RealmMode based on whether ZK address has been set
    boolean isZkAddressSet = _zkAddress != null && !_zkAddress.isEmpty();

    // If realmMode is single-realm (in other words, ZkAddress is needed) and zk address is not
    // given, then try to look up ZK address if ZK realm sharding key is set
    // If realmMode is multi-realm, make sure it's not tied to a single sharding key
    if (!isZkAddressSet && _realmAwareZkConnectionConfig.getZkRealmShardingKey() != null
        && !_realmAwareZkConnectionConfig.getZkRealmShardingKey().isEmpty()) {
      if (_realmMode == RealmAwareZkClient.RealmMode.SINGLE_REALM) {
        // Try to resolve the zk address using the zk path sharding key if given
        try {
          _zkAddress = resolveZkAddressWithShardingKey(_realmAwareZkConnectionConfig);
          isZkAddressSet = true;
        } catch (InvalidRoutingDataException e) {
          LOG.warn(
              "GenericZkHelixApiBuilder: ZkAddress is not set and failed to resolve ZkAddress with ZK path sharding key!",
              e);
        }
      } else if (_realmMode == RealmAwareZkClient.RealmMode.MULTI_REALM) {
        // Multi-realm and a single sharding key cannot coexist (by definition, multi-realm can access multiple sharding keys)
        throw new HelixException(
            "GenericZkHelixApiBuilder: Cannot have a ZK path sharding key in ConnectionConfig on multi-realm mode! Multi-realm accesses multiple sharding keys.");
      }
    }

    if (_realmMode == RealmAwareZkClient.RealmMode.SINGLE_REALM && !isZkAddressSet) {
      throw new HelixException(
          "GenericZkHelixApiBuilder: RealmMode cannot be single-realm without a valid ZkAddress set!");
    }
    if (_realmMode == RealmAwareZkClient.RealmMode.MULTI_REALM && isZkAddressSet) {
      throw new HelixException(
          "GenericZkHelixApiBuilder: ZkAddress cannot be set on multi-realm mode!");
    }

    if (_realmMode == null) {
      _realmMode = isZkAddressSet ? RealmAwareZkClient.RealmMode.SINGLE_REALM
          : RealmAwareZkClient.RealmMode.MULTI_REALM;
    }
  }

  /**
   * Initializes Realm-aware ZkConnection and ZkClient configs if they haven't been set.
   */
  protected void initializeConfigsIfNull() {
    // Resolve all default values
    if (_realmAwareZkConnectionConfig == null) {
      _realmAwareZkConnectionConfig =
          new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build();
    }

    // For Helix APIs, ZNRecord should be the default data model
    if (_realmAwareZkClientConfig == null) {
      _realmAwareZkClientConfig = new RealmAwareZkClient.RealmAwareZkClientConfig()
          .setZkSerializer(new ZNRecordSerializer());
    }
  }

  /**
   * Creates a RealmAwareZkClient based on the parameters set.
   * To be used in Helix ZK APIs' constructors: ConfigAccessor, ClusterSetup, ZKHelixAdmin
   * @return
   */
  protected RealmAwareZkClient createZkClient(RealmAwareZkClient.RealmMode realmMode,
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig clientConfig, String zkAddress) {
    switch (realmMode) {
      case MULTI_REALM:
        try {
          return new FederatedZkClient(connectionConfig,
              clientConfig.setZkSerializer(new ZNRecordSerializer()));
        } catch (InvalidRoutingDataException | IllegalStateException e) {
          throw new HelixException("GenericZkHelixApiBuilder: Failed to create FederatedZkClient!",
              e);
        }
      case SINGLE_REALM:
        // Create a HelixZkClient: Use a SharedZkClient because ClusterSetup does not need to do
        // ephemeral operations
        return SharedZkClientFactory.getInstance().buildZkClient(
            new HelixZkClient.ZkConnectionConfig(zkAddress)
                .setSessionTimeout(connectionConfig.getSessionTimeout()),
            clientConfig.createHelixZkClientConfig().setZkSerializer(new ZNRecordSerializer()));
      default:
        throw new HelixException("GenericZkHelixApiBuilder: Invalid RealmMode given: " + realmMode);
    }
  }

  /**
   * Returns an instance of a subclass-Builder in order to reduce duplicate code.
   * SuppressWarnings is used to rid of IDE warnings.
   * @return an instance of a subclass-Builder. E.g.) ConfigAccessor.Builder
   */
  @SuppressWarnings("unchecked")
  final B self() {
    return (B) this;
  }

  /**
   * Resolve Zk address based on the zk realm sharding key. This method is only used if the
   * ZK address is not given in this Builder.
   * @param connectionConfig
   * @return
   * @throws InvalidRoutingDataException
   */
  private String resolveZkAddressWithShardingKey(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig)
      throws InvalidRoutingDataException {
    MetadataStoreRoutingData routingData =
        RealmAwareZkClient.getMetadataStoreRoutingData(connectionConfig);
    return routingData.getMetadataStoreRealm(connectionConfig.getZkRealmShardingKey());
  }
}

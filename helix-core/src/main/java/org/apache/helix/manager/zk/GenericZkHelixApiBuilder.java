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

import org.apache.helix.HelixException;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;


/**
 * GenericZkHelixApiBuilder serves as the abstract parent class for Builders used by Helix Java APIs
 * that create ZK connections. By having this class, we reduce duplicate code as much as possible.
 * @param <B>
 */
public abstract class GenericZkHelixApiBuilder<B extends GenericZkHelixApiBuilder<B>> {
  private String _zkAddress;
  private RealmAwareZkClient.RealmMode _realmMode;
  private RealmAwareZkClient.RealmAwareZkConnectionConfig _realmAwareZkConnectionConfig;
  private RealmAwareZkClient.RealmAwareZkClientConfig _realmAwareZkClientConfig;

  public B setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return self();
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public B setRealmMode(RealmAwareZkClient.RealmMode realmMode) {
    _realmMode = realmMode;
    return self();
  }

  public RealmAwareZkClient.RealmMode getRealmMode() {
    return _realmMode;
  }

  public B setRealmAwareZkConnectionConfig(
      RealmAwareZkClient.RealmAwareZkConnectionConfig realmAwareZkConnectionConfig) {
    _realmAwareZkConnectionConfig = realmAwareZkConnectionConfig;
    return self();
  }

  public RealmAwareZkClient.RealmAwareZkConnectionConfig getRealmAwareZkConnectionConfig() {
    return _realmAwareZkConnectionConfig;
  }

  public B setRealmAwareZkClientConfig(
      RealmAwareZkClient.RealmAwareZkClientConfig realmAwareZkClientConfig) {
    _realmAwareZkClientConfig = realmAwareZkClientConfig;
    return self();
  }

  public RealmAwareZkClient.RealmAwareZkClientConfig getRealmAwareZkClientConfig() {
    return _realmAwareZkClientConfig;
  }

  /**
   * Validates the given Builder parameters using a generic validation logic.
   */
  protected void validate() {
    // Resolve RealmMode based on whether ZK address has been set
    boolean isZkAddressSet = _zkAddress != null && !_zkAddress.isEmpty();
    if (_realmMode == RealmAwareZkClient.RealmMode.SINGLE_REALM && !isZkAddressSet) {
      throw new HelixException("RealmMode cannot be single-realm without a valid ZkAddress set!");
    }
    if (_realmMode == RealmAwareZkClient.RealmMode.MULTI_REALM && isZkAddressSet) {
      throw new HelixException("ZkAddress cannot be set on multi-realm mode!");
    }
    if (_realmMode == null) {
      _realmMode = isZkAddressSet ? RealmAwareZkClient.RealmMode.SINGLE_REALM
          : RealmAwareZkClient.RealmMode.MULTI_REALM;
    }

    initializeConfigsIfNull();
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
   * Returns an instance of a subclass-Builder in order to reduce duplicate code.
   * SuppressWarnings is used to rid of IDE warnings.
   * @return an instance of a subclass-Builder. E.g.) ConfigAccessor.Builder
   */
  @SuppressWarnings("unchecked")
  final B self() {
    return (B) this;
  }
}

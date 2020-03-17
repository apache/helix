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


/**
 * GenericBaseDataAccessorBuilder serves as the abstract parent class for Builders used by
 * BaseDataAccessor APIs that create ZK connections. By having this class, we promote code-reuse.
 * @param <B>
 */
public class GenericBaseDataAccessorBuilder<B extends GenericBaseDataAccessorBuilder<B>> extends GenericZkHelixApiBuilder<B> {
  /** ZK-based BaseDataAccessor-specific parameter **/
  private ZkBaseDataAccessor.ZkClientType _zkClientType;

  /**
   * Sets the ZkClientType.
   * If this is set to either DEDICATED or SHARED, this accessor will be created on
   * single-realm mode.
   * If this is set to FEDERATED, multi-realm mode will be used.
   * @param zkClientType
   * @return
   */
  public B setZkClientType(ZkBaseDataAccessor.ZkClientType zkClientType) {
    _zkClientType = zkClientType;
    return self();
  }

  public ZkBaseDataAccessor.ZkClientType getZkClientType() {
    return _zkClientType;
  }

  /**
   * Validates the given parameters before building an instance of ZkBaseDataAccessor.
   */
  @Override
  protected void validate() {
    super.validate();
    validateZkClientType(_zkClientType, getRealmMode());
  }

  /**
   * Validate ZkClientType based on RealmMode.
   * @param zkClientType
   * @param realmMode
   */
  private void validateZkClientType(ZkBaseDataAccessor.ZkClientType zkClientType,
      RealmAwareZkClient.RealmMode realmMode) {
    boolean isZkClientTypeSet = zkClientType != null;
    // If ZkClientType is set, RealmMode must either be single-realm or not set.
    if (isZkClientTypeSet && realmMode == RealmAwareZkClient.RealmMode.MULTI_REALM) {
      throw new HelixException("ZkClientType cannot be set on multi-realm mode!");
    }
    // If ZkClientType is not set and realmMode is single-realm, default to SHARED
    if (!isZkClientTypeSet && realmMode == RealmAwareZkClient.RealmMode.SINGLE_REALM) {
      zkClientType = ZkBaseDataAccessor.ZkClientType.SHARED;
    }
    if (realmMode == RealmAwareZkClient.RealmMode.SINGLE_REALM
        && zkClientType == ZkBaseDataAccessor.ZkClientType.FEDERATED) {
      throw new HelixException("FederatedZkClient cannot be set on single-realm mode!");
    }
  }
}

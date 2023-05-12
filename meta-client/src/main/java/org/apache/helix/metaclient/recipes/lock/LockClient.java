package org.apache.helix.metaclient.recipes.lock;

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


import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.api.Op;
import org.apache.helix.metaclient.datamodel.DataRecord;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.factories.MetaClientFactory;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientFactory;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;

import java.util.Arrays;
import java.util.List;

public class LockClient implements LockClientInterface, AutoCloseable {
  private final MetaClientInterface<LockInfo> _metaClient;

  public LockClient(MetaClientConfig config) {
    if (config.getStoreType() == MetaClientConfig.StoreType.ZOOKEEPER) {
      ZkMetaClientConfig zkMetaClientConfig = new ZkMetaClientConfig.ZkMetaClientConfigBuilder().
          setConnectionAddress(config.getConnectionAddress())
          // Currently only support ZNRecordSerializer. TODO: make this configurable
          .setZkSerializer((new ZNRecordSerializer()))
          .build();
      _metaClient = new ZkMetaClientFactory().getMetaClient(zkMetaClientConfig);
    } else {
      throw new MetaClientException("Unsupported store type: " + config.getStoreType());
    }
    _metaClient.connect();
  }

  public LockClient(MetaClientInterface<LockInfo> client) {
    _metaClient = client;
    _metaClient.connect();
  }

  @Override
  public boolean acquireLock(String key, LockInfo lockInfo, MetaClientInterface.EntryMode mode) {
    _metaClient.create(key, lockInfo, mode);
    return true;
  }

  @Override
  public boolean acquireLockWithTTL(String key, LockInfo lockInfo, long ttl) {
    _metaClient.createWithTTL(key, lockInfo, ttl);
    return true;
  }

  @Override
  public boolean renewTTLLock(String key) {
    _metaClient.renewTTLNode(key);
    return true;
  }

  @Override
  public boolean releaseLock(String key) {
    MetaClientInterface.Stat stat = _metaClient.exists(key);
    if (stat != null) {
      int version = stat.getVersion();
      List<Op> ops = Arrays.asList(
          Op.check(key, version),
          Op.delete(key, version));
      _metaClient.transactionOP(ops);
      if (_metaClient.exists(key) != null) {
        throw new MetaClientException("Failed to release lock for key: " + key);
      }
    }
    return true;
  }

  @Override
  public LockInfo retrieveLock(String key) {
    MetaClientInterface.Stat stat = _metaClient.exists(key);
    if (stat == null) {
      return null;
    }
    //Create a new DataRecord from underlying record
    DataRecord dataRecord = new DataRecord(_metaClient.get(key));
    //Create a new LockInfo from DataRecord
    LockInfo lockInfo = new LockInfo(dataRecord);
    //Synchronize the lockInfo with the stat
    lockInfo.setGrantedAt(stat.getCreationTime());
    lockInfo.setLastRenewedAt(stat.getModifiedTime());
    return lockInfo;
  }

  @Override
  public void close() {
    _metaClient.disconnect();
  }
}

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
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientFactory;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class LockClient implements LockClientInterface, AutoCloseable {
  private final MetaClientInterface<LockInfo> _metaClient;
  //NEW_METACLIENT is used to indicate whether the metaClient is created by the LockClient or not.
  private static Boolean NEW_METACLIENT = false;
  private static final Logger LOG = LoggerFactory.getLogger(LockClient.class);

  public LockClient(MetaClientConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("MetaClientConfig cannot be null.");
    }
    LOG.info("Creating MetaClient for LockClient");
    if (MetaClientConfig.StoreType.ZOOKEEPER.equals(config.getStoreType())) {
      ZkMetaClientConfig zkMetaClientConfig = new ZkMetaClientConfig.ZkMetaClientConfigBuilder().
          setConnectionAddress(config.getConnectionAddress())
          // Currently only support ZNRecordSerializer. TODO: make this configurable
          .setZkSerializer((new ZNRecordSerializer()))
          .build();
      _metaClient = new ZkMetaClientFactory().getMetaClient(zkMetaClientConfig);
      _metaClient.connect();
      NEW_METACLIENT = true;
    } else {
      throw new MetaClientException("Unsupported store type: " + config.getStoreType());
    }
  }

  public LockClient(MetaClientInterface<LockInfo> client) {
    if (client == null) {
      throw new IllegalArgumentException("MetaClient cannot be null.");
    }
    _metaClient = client;
    try {
      LOG.info("Connecting to existing MetaClient for LockClient");
      _metaClient.connect();
    } catch (IllegalStateException e) {
      // Ignore as it either has already been connected or already been closed.
    }
  }

  @Override
  public void acquireLock(String key, LockInfo lockInfo, MetaClientInterface.EntryMode mode) {
    _metaClient.create(key, lockInfo, mode);
  }

  @Override
  public void acquireLockWithTTL(String key, LockInfo lockInfo, long ttl) {
    _metaClient.createWithTTL(key, lockInfo, ttl);
  }

  @Override
  public void renewTTLLock(String key) {
    _metaClient.renewTTLNode(key);
  }

  @Override
  public void releaseLock(String key) {
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
    LockInfo lockInfo = new LockInfo(dataRecord, stat);
    return lockInfo;
  }

  @Override
  public void close() {
    if (NEW_METACLIENT) {
      LOG.info("Closing created MetaClient for LockClient");
    } else {
      LOG.warn("Closing existing MetaClient");
    }
    _metaClient.disconnect();
  }
}

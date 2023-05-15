package org.apache.helix.metaclient.factories;

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
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientFactory;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory class for MetaClient. It returns MetaClient entity based on config.
 */
public class MetaClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MetaClientFactory.class);

  public MetaClientInterface getMetaClient(MetaClientConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("MetaClientConfig cannot be null.");
    }
    if (MetaClientConfig.StoreType.ZOOKEEPER.equals(config.getStoreType())) {
      ZkMetaClientConfig zkMetaClientConfig = new ZkMetaClientConfig.ZkMetaClientConfigBuilder().
          setConnectionAddress(config.getConnectionAddress())
          .setMetaClientReconnectPolicy(config.getMetaClientReconnectPolicy())
          .setConnectionInitTimeoutInMillis(config.getConnectionInitTimeoutInMillis())
          .setSessionTimeoutInMillis(config.getSessionTimeoutInMillis())
          .build();
      return new ZkMetaClientFactory().getMetaClient(zkMetaClientConfig);
    }
    return null;
  }
}

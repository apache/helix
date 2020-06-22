package org.apache.helix;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Realm;


/**
 * Singleton factory that builds different types of Helix property, e.g. Helix manager property.
 */
public final class HelixPropertyFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HelixPropertyFactory.class);
  private static final String HELIX_PARTICIPANT_PROPERTY_FILE =
      SystemPropertyKeys.HELIX_MANAGER_PROPERTIES;

  private static class SingletonHelper {
    private static final HelixPropertyFactory INSTANCE = new HelixPropertyFactory();
  }

  public static HelixPropertyFactory getInstance() {
    return SingletonHelper.INSTANCE;
  }

  /**
   * Retrieve Helix manager property. It returns the property object with default values.
   * Clients may override these values.
   */
  public HelixManagerProperty getHelixManagerProperty(String zkAddress, String clusterName) {
    CloudConfig cloudConfig;




    // If the multi ZK config is enabled, use FederatedZkClient on multi-realm mode
    if (Boolean.parseBoolean(System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED))) {
      try {
        RealmAwareZkClient zkClient = null;
        RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder builder = new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
        builder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM).setZkRealmShardingKey("/" + clusterName);

        zkClient = DedicatedZkClientFactory.getInstance().buildZkClient(builder.build());
        return;
      } catch (IOException | InvalidRoutingDataException | IllegalStateException e) {
        throw new HelixException("Failed to create ConfigAccessor!", e);
      }
    }

    _zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
            new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer()));






    try {
      // This configAccessor is a dedicated zkclient because HelixManager is single realm
      HelixZkClient.ZkConnectionConfig connectionConfig =
          new HelixZkClient.ZkConnectionConfig(zkAddress);
      zkClient = DedicatedZkClientFactory.getInstance().buildZkClient(connectionConfig);
      zkClient.setZkSerializer(new ZNRecordSerializer());
      ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
      // The try-catch logic is for backward compatibility reason only. Even if the cluster is not set
      // up yet, constructing a new ZKHelixManager should not throw an exception
      try {
        cloudConfig =
            configAccessor.getCloudConfig(clusterName) == null ? buildEmptyCloudConfig(clusterName)
                : configAccessor.getCloudConfig(clusterName);
      } catch (HelixException e) {
        cloudConfig = buildEmptyCloudConfig(clusterName);
      }
    } finally {
      // Use a try-finally to make sure zkclient connection is closed properly
      if (zkClient != null && !zkClient.isClosed()) {
        zkClient.close();
      }
    }

    Properties properties = new Properties();
    try {
      InputStream stream = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream(HELIX_PARTICIPANT_PROPERTY_FILE);
      properties.load(stream);
    } catch (IOException e) {
      String errMsg = String.format("failed to open Helix participant properties file: %s",
          HELIX_PARTICIPANT_PROPERTY_FILE);
      throw new IllegalArgumentException(errMsg, e);
    }
    LOG.info("HelixPropertyFactory successfully loaded helix participant properties: {}",
        properties);
    return new HelixManagerProperty(properties, cloudConfig);
  }

  public static CloudConfig buildEmptyCloudConfig(String clusterName) {
    return new CloudConfig.Builder().setCloudEnabled(false).build();
  }
}

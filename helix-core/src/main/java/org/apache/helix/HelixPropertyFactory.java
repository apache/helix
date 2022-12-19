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
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    CloudConfig cloudConfig = getCloudConfig(zkAddress, clusterName, null);
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

  /**
   * Retrieve the CloudConfig of the cluster if available.
   * Note: the reason we create a dedicated zk client here is because we need an isolated access to
   * ZK in order to create a HelixManager instance.
   * If shared zk client instance is used, this logic may break if users write tests that shut down
   * ZK server and start again at a 0 zxid because the shared client would have a higher zxid.
   * @param zkAddress
   * @param clusterName
   * @return
   */

  public static CloudConfig getCloudConfig(String zkAddress, String clusterName) {
    return getCloudConfig(zkAddress, clusterName, null);
  }
  public static CloudConfig getCloudConfig(String zkAddress, String clusterName,
      RealmAwareZkClient.RealmAwareZkConnectionConfig realmAwareZkConnectionConfig) {
    CloudConfig cloudConfig;
    RealmAwareZkClient dedicatedZkClient = null;
    try {
      if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED) || zkAddress == null) {
        // If the multi ZK config is enabled or zkAddress is null, use realm-aware mode with
        // DedicatedZkClient
        try {
          if (realmAwareZkConnectionConfig == null) {
            realmAwareZkConnectionConfig =
                new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
                    .setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
                    .setZkRealmShardingKey("/" + clusterName).build();
          }
          dedicatedZkClient =
              DedicatedZkClientFactory.getInstance().buildZkClient(realmAwareZkConnectionConfig);
        } catch (IOException | InvalidRoutingDataException e) {
          throw new HelixException("Not able to connect on multi-ZK mode!", e);
        }
      } else {
        // Use a dedicated ZK client single-ZK mode
        HelixZkClient.ZkConnectionConfig connectionConfig =
            new HelixZkClient.ZkConnectionConfig(zkAddress);
        dedicatedZkClient = DedicatedZkClientFactory.getInstance().buildZkClient(connectionConfig);
      }
      dedicatedZkClient.setZkSerializer(new ZNRecordSerializer());
      ConfigAccessor configAccessor = new ConfigAccessor(dedicatedZkClient);

      // The try-catch logic is for backward compatibility reason only. Even if the cluster is not set
      // up yet, constructing a new ZKHelixManager should not throw an exception
      try {
        cloudConfig = configAccessor.getCloudConfig(clusterName);
        if (cloudConfig == null) {
          cloudConfig = new CloudConfig();
        }
      } catch (HelixException e) {
        cloudConfig = new CloudConfig();
      }
    } finally {
      // Use a try-finally to make sure zkclient connection is closed properly
      if (dedicatedZkClient != null && !dedicatedZkClient.isClosed()) {
        dedicatedZkClient.close();
      }
    }
    return cloudConfig;
  }
}

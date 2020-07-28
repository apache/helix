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

import java.util.Properties;

import org.apache.helix.model.CloudConfig;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HelixManagerProperty is a general property/config object used for HelixManager creation.
 */
public class HelixManagerProperty {
  private static final Logger LOG = LoggerFactory.getLogger(HelixManagerProperty.class.getName());
  private String _version;
  private long _healthReportLatency;
  private HelixCloudProperty _helixCloudProperty;
  private RealmAwareZkClient.RealmAwareZkConnectionConfig _zkConnectionConfig;
  private RealmAwareZkClient.RealmAwareZkClientConfig _zkClientConfig;

  /**
   * ** Deprecated - HelixManagerProperty should be a general property/config object used for
   * HelixManager creation, not tied only to Properties or CloudConfig **
   *
   * Initialize Helix manager property with default value
   * @param helixManagerProperties helix manager related properties input as a map
   * @param cloudConfig cloudConfig read from Zookeeper
   */
  @Deprecated
  public HelixManagerProperty(Properties helixManagerProperties, CloudConfig cloudConfig) {
    _helixCloudProperty = new HelixCloudProperty(cloudConfig);
    setVersion(helixManagerProperties.getProperty(SystemPropertyKeys.HELIX_MANAGER_VERSION));
    setHealthReportLatency(
        helixManagerProperties.getProperty(SystemPropertyKeys.PARTICIPANT_HEALTH_REPORT_LATENCY));
  }

  public HelixManagerProperty() {
  }

  public HelixCloudProperty getHelixCloudProperty() {
    return _helixCloudProperty;
  }

  public void setHelixCloudProperty(HelixCloudProperty helixCloudProperty) {
    _helixCloudProperty = helixCloudProperty;
  }

  public String getVersion() {
    return _version;
  }

  public void setVersion(String version) {
    _version = version;
  }

  public long getHealthReportLatency() {
    return _healthReportLatency;
  }

  public void setHealthReportLatency(String latency) {
    _healthReportLatency = Long.valueOf(latency);
  }

  public RealmAwareZkClient.RealmAwareZkConnectionConfig getZkConnectionConfig() {
    return _zkConnectionConfig;
  }

  public void setZkConnectionConfig(
      RealmAwareZkClient.RealmAwareZkConnectionConfig zkConnectionConfig) {
    _zkConnectionConfig = zkConnectionConfig;
  }

  public RealmAwareZkClient.RealmAwareZkClientConfig getZkClientConfig() {
    return _zkClientConfig;
  }

  public void setZkClientConfig(RealmAwareZkClient.RealmAwareZkClientConfig zkClientConfig) {
    _zkClientConfig = zkClientConfig;
  }

  // TODO: migrate all other participant related properties to this file.
}

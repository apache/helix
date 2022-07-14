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

import org.apache.commons.lang3.StringUtils;
import org.apache.helix.messaging.handling.TaskExecutor;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HelixManagerProperty is a general property/config object used for HelixManager creation.
 */
public class HelixManagerProperty {
  private static final Logger LOG = LoggerFactory.getLogger(HelixManagerProperty.class.getName());
  private final String _version;
  private final long _healthReportLatency;
  private final int _msgHandlerResetTimeout;
  private HelixCloudProperty _helixCloudProperty;
  // if _zkAddr is set, _zkConnectionConfig cannot be set
  private final String _zkAddr;
  private final RealmAwareZkClient.RealmAwareZkConnectionConfig _zkConnectionConfig;
  private final RealmAwareZkClient.RealmAwareZkClientConfig _zkClientConfig;

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
    this(helixManagerProperties.getProperty(SystemPropertyKeys.HELIX_MANAGER_VERSION),
        Long.parseLong(
            helixManagerProperties.getProperty(SystemPropertyKeys.PARTICIPANT_HEALTH_REPORT_LATENCY)),
        null, TaskExecutor.DEFAULT_MSG_HANDLER_RESET_TIMEOUT_MS, new HelixCloudProperty(cloudConfig),
        null, null);
  }

  private HelixManagerProperty(String version, long healthReportLatency, String zkAddr, int msgHandlerResetTimeout,
      HelixCloudProperty helixCloudProperty,
      RealmAwareZkClient.RealmAwareZkConnectionConfig zkConnectionConfig,
      RealmAwareZkClient.RealmAwareZkClientConfig zkClientConfig) {
    if (StringUtils.isNotEmpty(zkAddr)) {
      if (zkConnectionConfig != null) {
        throw new IllegalArgumentException("Cannot have both zkAddress and ZkConnectionConfig set!");
      }
    } else if (zkConnectionConfig == null) {
      LOG.warn("Neither of zkAddr and zkConnectionConfig is specified.");
    }
    _version = version;
    _healthReportLatency = healthReportLatency;
    _zkAddr = zkAddr;
    _msgHandlerResetTimeout = msgHandlerResetTimeout;
    _helixCloudProperty = helixCloudProperty;
    _zkConnectionConfig = zkConnectionConfig;
    _zkClientConfig = zkClientConfig;
  }

  public HelixCloudProperty getHelixCloudProperty() {
    if (_helixCloudProperty == null) {
      _helixCloudProperty = new HelixCloudProperty(new CloudConfig());
    }
    return _helixCloudProperty;
  }

  public String getZkAddr() {
    return _zkAddr;
  }

  public String getVersion() {
    return _version;
  }

  public long getHealthReportLatency() {
    return _healthReportLatency;
  }

  public int getMsgHandlerResetTimeout() {
    return _msgHandlerResetTimeout;
  }

  public RealmAwareZkClient.RealmAwareZkConnectionConfig getZkConnectionConfig() {
    return _zkConnectionConfig;
  }

  public RealmAwareZkClient.RealmAwareZkClientConfig getZkClientConfig() {
    return _zkClientConfig;
  }

  public static class Builder {
    private String _version;
    private String _zkAddr;
    private long _healthReportLatency;
    private HelixCloudProperty _helixCloudProperty;
    private RealmAwareZkClient.RealmAwareZkConnectionConfig _zkConnectionConfig;
    private RealmAwareZkClient.RealmAwareZkClientConfig _zkClientConfig;
    private int _msgHandlerResetTimeout = TaskExecutor.DEFAULT_MSG_HANDLER_RESET_TIMEOUT_MS;

    public Builder() {
    }

    public HelixManagerProperty build() {
      return new HelixManagerProperty(_version, _healthReportLatency, _zkAddr, _msgHandlerResetTimeout,
          _helixCloudProperty, _zkConnectionConfig, _zkClientConfig);
    }

    public Builder setVersion(String version) {
      _version = version;
      return this;
    }

    public Builder setZkAddr(String zkAddr) {
      _zkAddr = zkAddr;
      return this;
    }

    public Builder setHealthReportLatency(long healthReportLatency) {
      _healthReportLatency = healthReportLatency;
      return this;
    }

    public Builder setHelixCloudProperty(HelixCloudProperty helixCloudProperty) {
      _helixCloudProperty = helixCloudProperty;
      return this;
    }

    public Builder setRealmAWareZkConnectionConfig(
        RealmAwareZkClient.RealmAwareZkConnectionConfig zkConnectionConfig) {
      _zkConnectionConfig = zkConnectionConfig;
      return this;
    }

    public Builder setRealmAwareZkClientConfig(
        RealmAwareZkClient.RealmAwareZkClientConfig zkClientConfig) {
      _zkClientConfig = zkClientConfig;
      return this;
    }

    public Builder setMsgHandlerResetTimeout(int msgHandlerResetTimeout) {
      _msgHandlerResetTimeout = msgHandlerResetTimeout;
      return this;
    }
  }
}

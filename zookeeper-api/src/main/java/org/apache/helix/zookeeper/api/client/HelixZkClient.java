package org.apache.helix.zookeeper.api.client;

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

import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;


/**
 * Deprecated - please use RealmAwareZkClient instead.
 *
 * HelixZkClient interface that follows the supported API structure of RealmAwareZkClient.
 */
@Deprecated
public interface HelixZkClient extends RealmAwareZkClient {

  /**
   * Deprecated - please use RealmAwareZkClient and RealmAwareZkConnectionConfig instead.
   *
   * Configuration for creating a new ZkConnection.
   */
  @Deprecated
  class ZkConnectionConfig {
    // Connection configs
    private final String _zkServers;
    private int _sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    public ZkConnectionConfig(String zkServers) {
      _zkServers = zkServers;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof ZkConnectionConfig)) {
        return false;
      }
      ZkConnectionConfig configObj = (ZkConnectionConfig) obj;
      return (_zkServers == null && configObj._zkServers == null || _zkServers != null && _zkServers
          .equals(configObj._zkServers)) && _sessionTimeout == configObj._sessionTimeout;
    }

    @Override
    public int hashCode() {
      return _sessionTimeout * 31 + _zkServers.hashCode();
    }

    @Override
    public String toString() {
      return (_zkServers + "_" + _sessionTimeout).replaceAll("[\\W]", "_");
    }

    public ZkConnectionConfig setSessionTimeout(Integer sessionTimeout) {
      this._sessionTimeout = sessionTimeout;
      return this;
    }

    public String getZkServers() {
      return _zkServers;
    }

    public int getSessionTimeout() {
      return _sessionTimeout;
    }
  }

  /**
   * Deprecated - please use RealmAwareZkClient and RealmAwareZkClientConfig instead.
   *
   * Configuration for creating a new HelixZkClient with serializer and monitor.
   */
  @Deprecated
  class ZkClientConfig extends RealmAwareZkClientConfig {
    @Override
    public ZkClientConfig setZkSerializer(PathBasedZkSerializer zkSerializer) {
      this._zkSerializer = zkSerializer;
      return this;
    }

    @Override
    public ZkClientConfig setZkSerializer(ZkSerializer zkSerializer) {
      this._zkSerializer = new BasicZkSerializer(zkSerializer);
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     *
     * @param monitorType
     */
    @Override
    public ZkClientConfig setMonitorType(String monitorType) {
      this._monitorType = monitorType;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     *
     * @param monitorKey
     */
    @Override
    public ZkClientConfig setMonitorKey(String monitorKey) {
      this._monitorKey = monitorKey;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is optional.
     *
     * @param instanceName
     */
    @Override
    public ZkClientConfig setMonitorInstanceName(String instanceName) {
      this._monitorInstanceName = instanceName;
      return this;
    }

    @Override
    public ZkClientConfig setMonitorRootPathOnly(Boolean monitorRootPathOnly) {
      this._monitorRootPathOnly = monitorRootPathOnly;
      return this;
    }

    @Override
    public ZkClientConfig setOperationRetryTimeout(Long operationRetryTimeout) {
      this._operationRetryTimeout = operationRetryTimeout;
      return this;
    }

    @Override
    public ZkClientConfig setConnectInitTimeout(long connectInitTimeout) {
      this._connectInitTimeout = connectInitTimeout;
      return this;
    }
  }
}

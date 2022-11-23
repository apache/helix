package org.apache.helix.metaclient.impl.zk.factory;

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

import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;


public class ZkMetaClientConfig extends MetaClientConfig {

  protected final PathBasedZkSerializer _zkSerializer;

  // Monitoring related fields. MBean names are crated using following variables in format of
  // MonitorPrefix_monitorType_monitorKey_monitorInstanceName, where _monitorInstanceName is optional
  // TODO: right now all zkClient mBean object has prefix `HelixZkClient` had coded. We should change
  // it to a configurable name.
  protected final String _monitorType;
  protected final String _monitorKey;
  protected final String _monitorInstanceName;
  protected final boolean _monitorRootPathOnly;

  public PathBasedZkSerializer getZkSerializer() {
    return _zkSerializer;
  }

  public String getMonitorType() {
    return _monitorType;
  }

  public String getMonitorKey() {
    return _monitorKey;
  }

  public String getMonitorInstanceName() {
    return _monitorInstanceName;
  }

  public boolean getMonitorRootPathOnly() {
    return _monitorRootPathOnly;
  }

  protected ZkMetaClientConfig(String connectionAddress, long connectionInitTimeoutInMillis,
      long sessionTimeoutInMillis, boolean enableAuth, StoreType storeType, String monitorType,
      String monitorKey, String monitorInstanceName, boolean monitorRootPathOnly,
      PathBasedZkSerializer zkSerializer) {
    super(connectionAddress, connectionInitTimeoutInMillis, sessionTimeoutInMillis, enableAuth,
        storeType);
    _zkSerializer = zkSerializer;
    _monitorType = monitorType;
    _monitorKey = monitorKey;
    _monitorInstanceName = monitorInstanceName;
    _monitorRootPathOnly = monitorRootPathOnly;

  }

  public static class ZkMetaClientConfigBuilder extends MetaClientConfig.MetaClientConfigBuilder<ZkMetaClientConfigBuilder> {

    protected PathBasedZkSerializer _zkSerializer;

    // Monitoring
    // Type as in MBean object
    protected String _monitorType;
    protected String _monitorKey;
    protected String _monitorInstanceName = null;
    protected boolean _monitorRootPathOnly = true;

    public ZkMetaClientConfigBuilder setZkSerializer(
        org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer zkSerializer) {
      this._zkSerializer = zkSerializer;
      return this;
    }

    public ZkMetaClientConfigBuilder setZkSerializer(ZkSerializer zkSerializer) {
      this._zkSerializer = new BasicZkSerializer(zkSerializer);
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     * @param monitorType
     */
    public ZkMetaClientConfigBuilder setMonitorType(String monitorType) {
      this._monitorType = monitorType;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     * @param monitorKey
     */
    public ZkMetaClientConfigBuilder setMonitorKey(String monitorKey) {
      this._monitorKey = monitorKey;
      return this;
    }

    /**
     * Used as part of the MBean ObjectName. This item is optional.
     * @param instanceName
     */
    public ZkMetaClientConfigBuilder setMonitorInstanceName(String instanceName) {
      this._monitorInstanceName = instanceName;
      return this;
    }

    public ZkMetaClientConfigBuilder setMonitorRootPathOnly(Boolean monitorRootPathOnly) {
      this._monitorRootPathOnly = monitorRootPathOnly;
      return this;
    }

    @Override
    public MetaClientConfig build() {
      if (_zkSerializer == null) {
        _zkSerializer = new BasicZkSerializer(new SerializableSerializer());
      }
      return new ZkMetaClientConfig(_connectionAddress, _connectionInitTimeoutInMillis,
          _sessionTimeoutInMillis, _enableAuth, MetaClientConfig.StoreType.ZOOKEEPER, _monitorType,
          _monitorKey, _monitorInstanceName, _monitorRootPathOnly, _zkSerializer);
    }

    @Override
    protected void validate() {
      super.validate();
    }
  }
}
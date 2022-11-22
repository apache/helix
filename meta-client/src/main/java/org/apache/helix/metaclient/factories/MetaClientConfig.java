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

import org.apache.helix.metaclient.constants.MetaClientConstants;

class MetaClientConfig {

  public enum StoreType {
    ZOOKEEPER, ETCD, CUSTOMIZED
  }

  private final String _connectionAddress;
  // Wait for init timeout time until connection is initiated
  private final long _connectionInitTimeout;

  // When a client becomes partitioned from the metadata service for more than session timeout,
  // new session will be established.
  private final long _sessionTimeout;

  private final boolean _enableAuth;
  private final StoreType _storeType;

  // Monitoring
  protected String _monitorType;
  protected String _monitorKey;
  protected String _monitorInstanceName = null;
  protected boolean _monitorRootPathOnly = true;

  public String getConnectionAddress() {
    return _connectionAddress;
  }

  public long getConnectionInitTimeout() {
    return _connectionInitTimeout;
  }

  public boolean isAuthEnabled() {
    return _enableAuth;
  }

  public StoreType getStoreType() {
    return _storeType;
  }

  public long getSessionTimeout() {
    return _sessionTimeout;
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


  // TODO: More options to add later
  // private boolean _autoReRegistWatcher;  // re-register one time watcher when set to true
  // private boolean _resetWatchWhenReConnect; // re-register previous existing watcher when reconnect
  //
  //  public enum RetryProtocol {
  //    NO_RETRY, EXP_BACK_OFF, CONST_RETRY_INTERVAL
  //  }
  //  private RetryProtocol _retryProtocol;


  protected MetaClientConfig(String connectionAddress, long connectionInitTimeout,
      long sessionTimeout, boolean enableAuth, StoreType storeType, String monitorType,
      String monitorKey, String monitorInstanceName, boolean monitorRootPathOnly) {
    _connectionAddress = connectionAddress;
    _connectionInitTimeout = connectionInitTimeout;
    _sessionTimeout = sessionTimeout;
    _enableAuth = enableAuth;
    _storeType = storeType;
    _monitorType = monitorType;
    _monitorKey = monitorKey;
    _monitorInstanceName = monitorInstanceName;
    _monitorRootPathOnly = monitorRootPathOnly;
  }

  public static class MetaClientConfigBuilder<B extends MetaClientConfigBuilder<B>> {
    protected String _connectionAddress;

    protected long _connectionInitTimeout;
    protected long _sessionTimeout;
    // protected long _operationRetryTimeout;
    // protected RetryProtocol _retryProtocol;
    protected boolean _enableAuth;
    protected StoreType _storeType;

    // Monitoring
    protected String _monitorType;
    protected String _monitorKey;
    protected String _monitorInstanceName = null;
    protected boolean _monitorRootPathOnly = true;

    public MetaClientConfig build() {
      validate();
      return new MetaClientConfig(_connectionAddress, _connectionInitTimeout, _sessionTimeout,
          _enableAuth, _storeType, _monitorType, _monitorKey, _monitorInstanceName,
          _monitorRootPathOnly);
    }

    public MetaClientConfigBuilder() {
      // set default values
      setAuthEnabled(false);
      setConnectionInitTimeout(MetaClientConstants.DEFAULT_CONNECTION_INIT_TIMEOUT);
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     *
     * @param monitorType
     */
    public B setMonitorType(String monitorType) {
      this._monitorType = monitorType;
      return self();
    }

    /**
     * Used as part of the MBean ObjectName. This item is required for enabling monitoring.
     *
     * @param monitorKey
     */
    public B setMonitorKey(String monitorKey) {
      this._monitorKey = monitorKey;
      return self();
    }

    /**
     * Used as part of the MBean ObjectName. This item is optional.
     *
     * @param instanceName
     */
    public B setMonitorInstanceName(String instanceName) {
      this._monitorInstanceName = instanceName;
      return self();
    }

    public B setMonitorRootPathOnly(Boolean monitorRootPathOnly) {
      this._monitorRootPathOnly = monitorRootPathOnly;
      return self();
    }

    public B setConnectionAddress(String connectionAddress) {
      _connectionAddress = connectionAddress;
      return self();
    }

    public B setAuthEnabled(Boolean enableAuth) {
      _enableAuth = enableAuth;
      return self();
    }

    /**
     *
     * @param timeout
     * @return
     */
    public B setConnectionInitTimeout(long timeout) {
      _connectionInitTimeout = timeout;
      return self();
    }

    /**
     *
     * @param timeout
     * @return
     */
    public B setSessionTimeout(long timeout) {
      _sessionTimeout = timeout;
      return self();
    }

    public B setStoreType(StoreType storeType) {
      _storeType = storeType;
      return self();
    }

    // public B setOperationRetryTimeout(long timeout) {
    //   _operationRetryTimeout = timeout;
    //   return self();
    // }

    @SuppressWarnings("unchecked")
    final B self() {
      return (B) this;
    }

    protected void validate() {
      if (_storeType == null || _connectionAddress == null) {
        throw new IllegalArgumentException(
            "MetaClientConfig.Builder: store type or connection string is null");
      }
    }
  }
}
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
import org.apache.helix.metaclient.policy.ExponentialBackoffReconnectPolicy;
import org.apache.helix.metaclient.policy.MetaClientReconnectPolicy;

public class MetaClientConfig {

  public enum StoreType {
    ZOOKEEPER, ETCD, CUSTOMIZED
  }

  private final String _connectionAddress;

  // Wait for init timeout time until connection is initiated
  private final long _connectionInitTimeoutInMillis;

  // When a client becomes partitioned from the metadata service for more than session timeout,
  // new session will be established when reconnect.
  private final long _sessionTimeoutInMillis;

  // Policy to define client re-establish connection behavior when the connection to underlying
  // metadata store is expired.
  private final MetaClientReconnectPolicy _metaClientReconnectPolicy;

  private final boolean _enableAuth;
  private final StoreType _storeType;

  public String getConnectionAddress() {
    return _connectionAddress;
  }

  public long getConnectionInitTimeoutInMillis() {
    return _connectionInitTimeoutInMillis;
  }

  public boolean isAuthEnabled() {
    return _enableAuth;
  }

  public StoreType getStoreType() {
    return _storeType;
  }

  public long getSessionTimeoutInMillis() {
    return _sessionTimeoutInMillis;
  }

  public MetaClientReconnectPolicy getMetaClientReconnectPolicy() {
    return _metaClientReconnectPolicy;
  }

  // TODO: More options to add later
  // private boolean _autoReRegistWatcher;  // re-register one time watcher when set to true
  // private boolean _resetWatchWhenReConnect; // re-register previous existing watcher when reconnect

  protected MetaClientConfig(String connectionAddress, long connectionInitTimeoutInMillis,
      long sessionTimeoutInMillis, MetaClientReconnectPolicy metaClientReconnectPolicy,
      boolean enableAuth, StoreType storeType) {
    _connectionAddress = connectionAddress;
    _connectionInitTimeoutInMillis = connectionInitTimeoutInMillis;
    _sessionTimeoutInMillis = sessionTimeoutInMillis;
    _metaClientReconnectPolicy = metaClientReconnectPolicy;
    _enableAuth = enableAuth;
    _storeType = storeType;
  }

  public static class MetaClientConfigBuilder<B extends MetaClientConfigBuilder<B>> {
    protected String _connectionAddress;

    protected long _connectionInitTimeoutInMillis;
    protected long _sessionTimeoutInMillis;
    protected boolean _enableAuth;
    protected StoreType _storeType;
    protected MetaClientReconnectPolicy _metaClientReconnectPolicy;


    public MetaClientConfig build() {
      validate();
      return new MetaClientConfig(_connectionAddress, _connectionInitTimeoutInMillis,
          _sessionTimeoutInMillis, _metaClientReconnectPolicy, _enableAuth, _storeType);
    }

    public MetaClientConfigBuilder() {
      // set default values
      setStoreType(StoreType.ZOOKEEPER);
      setAuthEnabled(false);
      setConnectionInitTimeoutInMillis(MetaClientConstants.DEFAULT_CONNECTION_INIT_TIMEOUT_MS);
      setSessionTimeoutInMillis(MetaClientConstants.DEFAULT_SESSION_TIMEOUT_MS);
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
     * Set timeout in ms for connection initialization timeout
     * @param timeout
     * @return
     */
    public B setConnectionInitTimeoutInMillis(long timeout) {
      _connectionInitTimeoutInMillis = timeout;
      return self();
    }

    /**
     * Set reconnect policy when connection is lost or expired. By default is
     * ExponentialBackoffReconnectPolicy
     * @param reconnectPolicy an instance of type MetaClientReconnectPolicy
     * @return
     */
    public B setMetaClientReconnectPolicy(MetaClientReconnectPolicy reconnectPolicy) {
      _metaClientReconnectPolicy = reconnectPolicy;
      return self();
    }

    /**
     * Set timeout in mm for session timeout. When a client becomes partitioned from the metadata
     * service for more than session timeout, new session will be established.
     * @param timeout
     * @return
     */
    public B setSessionTimeoutInMillis(long timeout) {
      _sessionTimeoutInMillis = timeout;
      return self();
    }

    public B setStoreType(StoreType storeType) {
      _storeType = storeType;
      return self();
    }

    @SuppressWarnings("unchecked")
    final B self() {
      return (B) this;
    }

    protected void validate() {
      if (_metaClientReconnectPolicy == null) {
        _metaClientReconnectPolicy = new ExponentialBackoffReconnectPolicy();
      }

      if (_storeType == null || _connectionAddress == null) {
        throw new IllegalArgumentException(
            "MetaClientConfig.Builder: store type or connection string is null");
      }
    }
  }
}
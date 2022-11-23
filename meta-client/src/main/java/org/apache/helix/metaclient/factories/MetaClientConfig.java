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

  // TODO: More options to add later
  // private boolean _autoReRegistWatcher;  // re-register one time watcher when set to true
  // private boolean _resetWatchWhenReConnect; // re-register previous existing watcher when reconnect
  //
  //  public enum RetryProtocol {
  //    NO_RETRY, EXP_BACK_OFF, CONST_RETRY_INTERVAL
  //  }
  //  private RetryProtocol _retryProtocol;


  protected MetaClientConfig(String connectionAddress, long connectionInitTimeoutInMillis,
      long sessionTimeoutInMillis, boolean enableAuth, StoreType storeType) {
    _connectionAddress = connectionAddress;
    _connectionInitTimeoutInMillis = connectionInitTimeoutInMillis;
    _sessionTimeoutInMillis = sessionTimeoutInMillis;
    _enableAuth = enableAuth;
    _storeType = storeType;
  }

  public static class MetaClientConfigBuilder<B extends MetaClientConfigBuilder<B>> {
    protected String _connectionAddress;

    protected long _connectionInitTimeoutInMillis;
    protected long _sessionTimeoutInMillis;
    // protected long _operationRetryTimeout;
    // protected RetryProtocol _retryProtocol;
    protected boolean _enableAuth;
    protected StoreType _storeType;


    public MetaClientConfig build() {
      validate();
      return new MetaClientConfig(_connectionAddress, _connectionInitTimeoutInMillis,
          _sessionTimeoutInMillis,
          _enableAuth, _storeType);
    }

    public MetaClientConfigBuilder() {
      // set default values
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
     * Set timeout in mm for connection initialization timeout
     * @param timeout
     * @return
     */
    public B setConnectionInitTimeoutInMillis(long timeout) {
      _connectionInitTimeoutInMillis = timeout;
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
      if (_storeType == null || _connectionAddress == null) {
        throw new IllegalArgumentException(
            "MetaClientConfig.Builder: store type or connection string is null");
      }
    }
  }
}
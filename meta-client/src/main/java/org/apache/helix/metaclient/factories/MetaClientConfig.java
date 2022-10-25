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

class MetaClientConfig {

  public enum RetryProtocol {
    NO_RETRY, EXP_BACK_OFF, CONST_RETRY_INTERVAL
  }

  public enum StoreType {
    ZOOKEEPER, ETCD
  }

  private String _connectionAddress;
  private long _connectionTimeout;
  private boolean _enableAuth;
  private StoreType _storeType;

  public String getConnectionAddress() {
    return _connectionAddress;
  }

  public long getConnectionTimeout() {
    return _connectionTimeout;
  }

  public boolean isAuthEnabled() {
    return _enableAuth;
  }
  public StoreType getStoreType() {
    return _storeType;
  }

  // TODO: More options to add later
  // private boolean _autoReRegistWatcher;  // re-register one time watcher when set to true
  // private boolean _resetWatchWhenReConnect; // re-register previous existing watcher when reconnect
  // private RetryProtocol _retryProtocol;

  private MetaClientConfig(Builder builder) {
    _connectionAddress = builder._connectionAddress;
    _connectionTimeout = builder._connectionTimeout;
    _enableAuth = builder._enableAuth;
    _storeType = builder._storeType;

  }

  public static class Builder {
    private String _connectionAddress;

    private long _connectionTimeout;
    private boolean _enableAuth;
    //private RetryProtocol _retryProtocol;
    private StoreType _storeType;



    public MetaClientConfig build() {
      validate();
      return new MetaClientConfig(this);
    }

    public Builder() {
    }

    public Builder setConnectionAddress(String connectionAddress) {
      _connectionAddress = connectionAddress;
      return this;
    }

    public Builder setAuthEnabled(Boolean enableAuth) {
      _enableAuth = enableAuth;
      return this;
    }

    public Builder setConnectionTimeout(long timeout) {
      _connectionTimeout = timeout;
      return this;
    }

    public Builder setStoreType(StoreType storeType) {
      _storeType = storeType;
      return this;
    }

    private void validate() {
      if (_storeType == null || _connectionAddress == null) {
        throw new IllegalArgumentException(
            "MetaClientConfig.Builder: store type or connection string is null");
      }
    }
  }
}
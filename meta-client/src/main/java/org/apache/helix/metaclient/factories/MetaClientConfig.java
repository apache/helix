package org.apache.helix.metaclient.factories;

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
package org.apache.helix.metaclient.impl.zk.factory;

import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;


public class ZkMetaClientConfig extends MetaClientConfig {

  protected PathBasedZkSerializer _zkSerializer;

  // Monitoring related fields. MBean names are crated using following variables in format of
  // MonitorPrefix_monitorType_monitorKey_monitorInstanceName, where _monitorInstanceName is optional
  // TODO: right now all zkClient mBean object has prefix `HelixZkClient` had coded. We should change
  // it to a configurable name.
  protected String _monitorType;
  protected String _monitorKey;
  protected String _monitorInstanceName = null;
  protected boolean _monitorRootPathOnly = true;

  public PathBasedZkSerializer getZkSerializer() {
    if (_zkSerializer == null) {
      _zkSerializer = new BasicZkSerializer(new SerializableSerializer());
    }
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

  protected ZkMetaClientConfig(String connectionAddress, long connectionInitTimeout,
      long sessionTimeout, boolean enableAuth, StoreType storeType, String monitorType,
      String monitorKey, String monitorInstanceName, boolean monitorRootPathOnly,PathBasedZkSerializer zkSerializer
      ) {
    super(connectionAddress, connectionInitTimeout, sessionTimeout, enableAuth,
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
      return new ZkMetaClientConfig(_connectionAddress, _connectionInitTimeout, _sessionTimeout,
           _enableAuth, MetaClientConfig.StoreType.ZOOKEEPER, _monitorType,
          _monitorKey, _monitorInstanceName, _monitorRootPathOnly, _zkSerializer);
    }

    @Override
    protected void validate() {
      super.validate();
    }
  }
}
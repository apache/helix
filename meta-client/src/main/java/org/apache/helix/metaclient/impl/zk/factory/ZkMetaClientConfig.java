package org.apache.helix.metaclient.impl.zk.factory;

import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.zookeeper.zkclient.serialize.BasicZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.SerializableSerializer;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;


public class ZkMetaClientConfig extends MetaClientConfig {

  protected PathBasedZkSerializer _zkSerializer;


  public PathBasedZkSerializer getZkSerializer() {
    if (_zkSerializer == null) {
      _zkSerializer = new BasicZkSerializer(new SerializableSerializer());
    }
    return _zkSerializer;
  }
  protected ZkMetaClientConfig(String connectionAddress, long connectionInitTimeout,
      long sessionTimeout, boolean enableAuth, StoreType storeType, String monitorType,
      String monitorKey, String monitorInstanceName, boolean monitorRootPathOnly,PathBasedZkSerializer zkSerializer
      ) {
    super(connectionAddress, connectionInitTimeout, sessionTimeout, enableAuth,
        storeType, monitorType, monitorKey, monitorInstanceName, monitorRootPathOnly);
   _zkSerializer = zkSerializer;

  }

  public static class ZkMetaClientConfigBuilder extends MetaClientConfig.MetaClientConfigBuilder<ZkMetaClientConfigBuilder> {

    protected PathBasedZkSerializer _zkSerializer;

    public ZkMetaClientConfigBuilder setZkSerializer(
        org.apache.helix.zookeeper.zkclient.serialize.PathBasedZkSerializer zkSerializer) {
      this._zkSerializer = zkSerializer;
      return this;
    }

    public ZkMetaClientConfigBuilder setZkSerializer(ZkSerializer zkSerializer) {
      this._zkSerializer = new BasicZkSerializer(zkSerializer);
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

    }
  }
}
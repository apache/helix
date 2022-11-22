package org.apache.helix.metaclient.impl.zk.factory;

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.factories.MetaClientFactory;
import org.apache.helix.metaclient.impl.zk.ZkMetaClient;


public class ZkMetaClientFactory extends MetaClientFactory {
  @Override
  public MetaClientInterface getMetaClient(MetaClientConfig config) {
    if (config.getStoreType() == MetaClientConfig.StoreType.ZOOKEEPER
        && config instanceof ZkMetaClientConfig) {
      return new ZkMetaClient((ZkMetaClientConfig) config);
    }
    return null;
  }
}

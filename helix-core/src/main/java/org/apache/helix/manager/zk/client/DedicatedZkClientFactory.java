package org.apache.helix.manager.zk.client;

import org.apache.helix.manager.zk.ZkClient;

/**
 * Singleton factory that build dedicated clients using the raw ZkClient.
 */
public class DedicatedZkClientFactory extends HelixZkClientFactory {

  protected DedicatedZkClientFactory() {}

  private static class SingletonHelper{
    private static final DedicatedZkClientFactory INSTANCE = new DedicatedZkClientFactory();
  }

  public static DedicatedZkClientFactory getInstance(){
    return SingletonHelper.INSTANCE;
  }

  /**
   * Build a Dedicated ZkClient based on connection config and client config
   *
   * @param connectionConfig
   * @param clientConfig
   * @return
   */
  @Override
  public HelixZkClient buildZkClient(HelixZkClient.ZkConnectionConfig connectionConfig,
      HelixZkClient.ZkClientConfig clientConfig) {
    return new ZkClient(createZkConnection(connectionConfig),
        (int) clientConfig.getConnectInitTimeout(), clientConfig.getOperationRetryTimeout(),
        clientConfig.getZkSerializer(), clientConfig.getMonitorType(), clientConfig.getMonitorKey(),
        clientConfig.getMonitorInstanceName(), clientConfig.isMonitorRootPathOnly());
  }
}

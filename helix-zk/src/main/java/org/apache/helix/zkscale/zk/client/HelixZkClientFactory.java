package org.apache.helix.zkscale.zk.client;

import org.I0Itec.zkclient.IZkConnection;
//import org.apache.helix.HelixException;
import org.apache.helix.zkscale.zk.ZkScaleException;
import org.apache.helix.zkscale.zk.zookeeper.ZkConnection;

/**
 * Abstract class of the ZkClient factory.
 */
abstract class HelixZkClientFactory {

  /**
   * Build a ZkClient using specified connection config and client config
   *
   * @param connectionConfig
   * @param clientConfig
   * @return HelixZkClient
   */
  public abstract HelixZkClient buildZkClient(HelixZkClient.ZkConnectionConfig connectionConfig,
      HelixZkClient.ZkClientConfig clientConfig);

  /**
   * Build a ZkClient using specified connection config and default client config
   *
   * @param connectionConfig
   * @return HelixZkClient
   */
  public HelixZkClient buildZkClient(HelixZkClient.ZkConnectionConfig connectionConfig) {
    return buildZkClient(connectionConfig, new HelixZkClient.ZkClientConfig());
  }

  /**
   * Construct a new ZkConnection instance based on connection configuration.
   * Note that the connection is not really made until someone calls zkConnection.connect().
   * @param connectionConfig
   * @return
   */
  protected IZkConnection createZkConnection(HelixZkClient.ZkConnectionConfig connectionConfig) {
    if (connectionConfig.getZkServers() == null) {
      throw new ZkScaleException(
          "Failed to build ZkClient since no connection or ZK server address is specified.");
    } else {
      return new ZkConnection(connectionConfig.getZkServers(), connectionConfig.getSessionTimeout());
    }
  }
}


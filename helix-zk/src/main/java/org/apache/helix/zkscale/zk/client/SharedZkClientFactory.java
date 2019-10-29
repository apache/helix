package org.apache.helix.zkscale.zk.client;

import java.util.HashMap;

//import org.apache.helix.HelixException;
import org.apache.helix.zkscale.zk.ZkScaleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton factory that build shared ZkClient which use a shared ZkConnection.
 */
public class SharedZkClientFactory extends HelixZkClientFactory {
  private static Logger LOG = LoggerFactory.getLogger(SharedZkClient.class);
  // The connection pool to track all created connections.
  private final HashMap<HelixZkClient.ZkConnectionConfig, ZkConnectionManager>
      _connectionManagerPool = new HashMap<>();

  protected SharedZkClientFactory() {}

  private static class SingletonHelper {
    private static final SharedZkClientFactory INSTANCE = new SharedZkClientFactory();
  }

  public static SharedZkClientFactory getInstance() {
    return SingletonHelper.INSTANCE;
  }

  /**
   * Build a Shared ZkClient that uses sharing ZkConnection that is created based on the specified connection config.
   *
   * @param connectionConfig The connection configuration that is used to search for a shared connection. Or create new connection if necessary.
   * @param clientConfig
   * @return Shared ZkClient
   */
  @Override
  public HelixZkClient buildZkClient(HelixZkClient.ZkConnectionConfig connectionConfig,
      HelixZkClient.ZkClientConfig clientConfig) {
    synchronized (_connectionManagerPool) {
      final ZkConnectionManager zkConnectionManager =
          getOrCreateZkConnectionNamanger(connectionConfig, clientConfig.getConnectInitTimeout());
      if (zkConnectionManager == null) {
        throw new ZkScaleException("Failed to create a connection manager in the pool to share.");
      }
      LOG.info("Sharing ZkConnection {} to a new SharedZkClient.", connectionConfig.toString());
      return new SharedZkClient(zkConnectionManager, clientConfig,
          new SharedZkClient.OnCloseCallback() {
            @Override
            public void onClose() {
              cleanupConnectionManager(zkConnectionManager);
            }
          });
    }
  }

  private ZkConnectionManager getOrCreateZkConnectionNamanger(
      HelixZkClient.ZkConnectionConfig connectionConfig, long connectInitTimeout) {
    ZkConnectionManager connectionManager = _connectionManagerPool.get(connectionConfig);
    if (connectionManager == null || connectionManager.isClosed()) {
      connectionManager = new ZkConnectionManager(createZkConnection(connectionConfig), connectInitTimeout,
          connectionConfig.toString());
      _connectionManagerPool.put(connectionConfig, connectionManager);
    }
    return connectionManager;
  }

  // Close the ZkConnectionManager if no other shared client is referring to it.
  // Note the close operation of connection manager needs to be synchronized with the pool operation
  // to avoid race condition.
  private void cleanupConnectionManager(ZkConnectionManager zkConnectionManager) {
    synchronized (_connectionManagerPool) {
      zkConnectionManager.close(true);
    }
  }

  // For test only
  protected int getActiveConnectionCount() {
    int count = 0;
    synchronized (_connectionManagerPool) {
      for (ZkConnectionManager manager : _connectionManagerPool.values()) {
        if (!manager.isClosed()) {
          count++;
        }
      }
    }
    return count;
  }
}
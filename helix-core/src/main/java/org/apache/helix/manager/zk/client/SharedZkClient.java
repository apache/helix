package org.apache.helix.manager.zk.client;

import java.util.List;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.HelixException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZkClient that uses shared ZkConnection.
 * A SharedZkClient won't manipulate the shared ZkConnection directly.
 */
class SharedZkClient extends org.apache.helix.manager.zk.ZkClient implements HelixZkClient {
  private static Logger LOG = LoggerFactory.getLogger(SharedZkClient.class);
  /*
   * Since we cannot really disconnect the ZkConnection, we need a dummy ZkConnection placeholder.
   * This is to ensure connection field is never null even the shared ZkClient instance is closed so as to avoid NPE.
   */
  private final static ZkConnection IDLE_CONNECTION = new ZkConnection("Dummy_ZkServers");
  private final OnCloseCallback _onCloseCallback;
  private final ZkConnectionManager _connectionManager;

  interface OnCloseCallback {
    /**
     * Triggered after the ZkClient is closed.
     */
    void onClose();
  }

  /**
   * Construct a shared ZkClient that uses a shared ZkConnection.
   *
   * @param connectionManager     The manager of the shared ZkConnection.
   * @param clientConfig          ZkClientConfig details to create the shared ZkClient.
   * @param callback              Clean up logic when the shared ZkClient is closed.
   */
  protected SharedZkClient(ZkConnectionManager connectionManager, ZkClientConfig clientConfig,
      OnCloseCallback callback) {
    super(connectionManager.getConnection(), 0, clientConfig.getOperationRetryTimeout(),
        clientConfig.getZkSerializer(), clientConfig.getMonitorType(), clientConfig.getMonitorKey(),
        clientConfig.getMonitorInstanceName(), clientConfig.isMonitorRootPathOnly());
    _connectionManager = connectionManager;
    // Register to the base dedicated ZkClient
    _connectionManager.registerWatcher(this);
    _onCloseCallback = callback;
  }

  @Override
  public void close() {
    super.close();
    if (isClosed()) {
      // Note that if register is not done while constructing, these private fields may not be init yet.
      if (_connectionManager != null) {
        _connectionManager.unregisterWatcher(this);
      }
      if (_onCloseCallback != null) {
        _onCloseCallback.onClose();
      }
    }
  }

  @Override
  public IZkConnection getConnection() {
    if (isClosed()) {
      return IDLE_CONNECTION;
    }
    return super.getConnection();
  }

  /**
   * Since ZkConnection session is shared in this ZkClient, do not create ephemeral node using a SharedZKClient.
   */
  @Override
  public String create(final String path, Object datat, final List<ACL> acl,
      final CreateMode mode) {
    if (mode.isEphemeral()) {
      throw new HelixException(
          "Create ephemeral nodes using a " + SharedZkClient.class.getSimpleName()
              + " ZkClient is not supported.");
    }
    return super.create(path, datat, acl, mode);
  }

  @Override
  protected boolean isManagingZkConnection() {
    return false;
  }
}

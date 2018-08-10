package org.apache.helix.manager.zk.client;

import java.util.HashSet;
import java.util.Set;

import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.BasicZkSerializer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ZkConnection manager that maintain connection status and allows additional watchers to be registered.
 * It will forward events to those watchers.
 *
 * TODO Separate connection management logic from the raw ZkClient class.
 * So this manager is a peer to the ZkClient. Connection Manager for maintaining the connection and
 * ZkClient to handle user request.
 * After this is done, Dedicated ZkClient hires one manager for it's connection.
 * While multiple Shared ZkClients can use single connection manager if possible.
 */
class ZkConnectionManager extends org.apache.helix.manager.zk.ZkClient {
  private static Logger LOG = LoggerFactory.getLogger(ZkConnectionManager.class);
  // Client type that is used in monitor, and metrics.
  private final static String MONITOR_TYPE = "ZkConnectionManager";
  private final String _monitorKey;
  // Set of all registered watchers
  private final Set<Watcher> _sharedWatchers = new HashSet<>();

  /**
   * Construct and init a ZkConnection Manager.
   *
   * @param zkConnection
   * @param connectionTimeout
   */
  protected ZkConnectionManager(IZkConnection zkConnection, long connectionTimeout,
      String monitorKey) {
    super(zkConnection, (int) connectionTimeout, HelixZkClient.DEFAULT_OPERATION_TIMEOUT,
        new BasicZkSerializer(new SerializableSerializer()), MONITOR_TYPE, monitorKey, null, true);
    _monitorKey = monitorKey;
    LOG.info("ZkConnection {} was created for sharing.", _monitorKey);
  }

  /**
   * Register event watcher.
   *
   * @param watcher
   * @return true if the watcher is newly added. false if it is already in record.
   */
  protected synchronized boolean registerWatcher(Watcher watcher) {
    if (isClosed()) {
      throw new HelixException("Cannot add watcher to a closed client.");
    }
    return _sharedWatchers.add(watcher);
  }

  /**
   * Unregister the event watcher.
   *
   * @param watcher
   * @return number of the remaining event watchers
   */
  protected synchronized int unregisterWatcher(Watcher watcher) {
    _sharedWatchers.remove(watcher);
    return _sharedWatchers.size();
  }

  @Override
  public void process(final WatchedEvent event) {
    super.process(event);
    forwardingEvent(event);
  }

  private synchronized void forwardingEvent(final WatchedEvent event) {
    // note that process (then forwardingEvent) could be triggered during construction, when sharedWatchers is still null.
    if (_sharedWatchers == null || _sharedWatchers.isEmpty()) {
      return;
    }
    // forward event to all the watchers' event queue
    for (final Watcher watcher : _sharedWatchers) {
      watcher.process(event);
    }
  }

  @Override
  public void close() {
    // Enforce closing, if any watcher exists, throw Exception.
    close(false);
  }

  protected synchronized void close(boolean skipIfWatched) {
    cleanupInactiveWatchers();
    if (_sharedWatchers.size() > 0) {
      if (skipIfWatched) {
        LOG.debug("Skip closing ZkConnection due to existing watchers. Watcher count {}.",
            _sharedWatchers.size());
        return;
      } else {
        throw new HelixException(
            "Cannot close the connection when there are still shared watchers listen on the event.");
      }
    }
    super.close();
    LOG.info("ZkConnection {} was closed.", _monitorKey);
  }

  private void cleanupInactiveWatchers() {
    Set<Watcher> closedWatchers = new HashSet<>();
    for (Watcher watcher : _sharedWatchers) {
      // TODO ideally, we shall have a ClosableWatcher interface so as to check accordingly. -- JJ
      if (watcher instanceof SharedZkClient && ((SharedZkClient) watcher).isClosed()) {
        closedWatchers.add(watcher);
      }
    }
    _sharedWatchers.removeAll(closedWatchers);
  }
}

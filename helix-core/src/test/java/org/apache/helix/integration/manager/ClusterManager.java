package org.apache.helix.integration.manager;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterManager extends ZKHelixManager implements Runnable, ZkTestManager {
  private static Logger LOG = LoggerFactory.getLogger(ClusterControllerManager.class);

  protected CountDownLatch _startCountDown = new CountDownLatch(1);
  protected CountDownLatch _stopCountDown = new CountDownLatch(1);
  protected CountDownLatch _waitStopFinishCountDown = new CountDownLatch(1);

  protected boolean _started = false;

  public ClusterManager(String zkAddr, String clusterName, InstanceType type) {
    this(zkAddr, clusterName, "role", type);
  }

  public ClusterManager(String zkAddr, String clusterName, String roleName, InstanceType type) {
    super(clusterName, roleName, type, zkAddr);
  }

  public void syncStop() {
    _stopCountDown.countDown();
    try {
      _waitStopFinishCountDown.await();
      _started = false;
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for finish", e);
    }
  }

  // This should not be called more than once because HelixManager.connect() should not be called more than once.
  public void syncStart() {
    if (_started) {
      throw new RuntimeException(
          "Helix Controller already started. Do not call syncStart() more than once.");
    } else {
      _started = true;
    }

    new Thread(this).start();
    try {
      _startCountDown.await();
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for start", e);
    }
  }

  @Override
  public void run() {
    try {
      connect();
      _startCountDown.countDown();
      _stopCountDown.await();
    } catch (Exception e) {
      LOG.error("exception running controller-manager", e);
    } finally {
      _startCountDown.countDown();
      disconnect();
      _waitStopFinishCountDown.countDown();
    }
  }

  @Override
  public RealmAwareZkClient getZkClient() {
    return _zkclient;
  }

  @Override
  public List<CallbackHandler> getHandlers() {
    return _handlers;
  }
}


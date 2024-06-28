package org.apache.helix.gateway.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerManager extends ZKHelixManager implements Runnable {
  private static final int DISCONNECT_WAIT_TIME_MS = 3000;
  private static Logger logger = LoggerFactory.getLogger(ControllerManager.class);
  private static AtomicLong uid = new AtomicLong(10000);
  private final String _clusterName;
  private final String _instanceName;
  private final InstanceType _type;
  protected CountDownLatch _startCountDown = new CountDownLatch(1);
  protected CountDownLatch _stopCountDown = new CountDownLatch(1);
  protected CountDownLatch _waitStopFinishCountDown = new CountDownLatch(1);
  protected boolean _started = false;
  protected Thread _watcher;
  private long _uid;

  public ControllerManager(String zkAddr, String clusterName, String instanceName,
      InstanceType type) {
    super(clusterName, instanceName, type, zkAddr);
    _clusterName = clusterName;
    _instanceName = instanceName;
    _type = type;
    _uid = uid.getAndIncrement();
  }

  protected ControllerManager(String clusterName, String instanceName, InstanceType instanceType,
      String zkAddress, HelixManagerStateListener stateListener,
      HelixManagerProperty helixManagerProperty) {
    super(clusterName, instanceName, instanceType, zkAddress, stateListener, helixManagerProperty);
    _clusterName = clusterName;
    _instanceName = instanceName;
    _type = instanceType;
    _uid = uid.getAndIncrement();
  }

  public void syncStop() {
    _stopCountDown.countDown();
    try {
      _waitStopFinishCountDown.await();
      _started = false;
    } catch (InterruptedException e) {
      logger.error("Interrupted waiting for finish", e);
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

    _watcher = new Thread(this);
    _watcher.setName(
        String.format("ClusterManager_Watcher_%s_%s_%s_%d", _clusterName, _instanceName,
            _type.name(), _uid));
    logger.debug("ClusterManager_watcher_{}_{}_{}_{} started, stacktrace {}", _clusterName,
        _instanceName, _type.name(), _uid, Thread.currentThread().getStackTrace());
    _watcher.start();

    try {
      _startCountDown.await();
    } catch (InterruptedException e) {
      logger.error("Interrupted waiting for start", e);
    }
  }

  @Override
  public void run() {
    try {
      connect();
      _startCountDown.countDown();
      _stopCountDown.await();
    } catch (Exception e) {
      logger.error("exception running controller-manager", e);
    } finally {
      _startCountDown.countDown();
      disconnect();
      _waitStopFinishCountDown.countDown();
    }
  }

  /**
   @SuppressWarnings("finalizer")
   @Override public void finalize() {
   _watcher.interrupt();
   try {
   _watcher.join(DISCONNECT_WAIT_TIME_MS);
   } catch (InterruptedException e) {
   logger.error("ClusterManager watcher cleanup in the finalize method was interrupted.", e);
   } finally {
   if (isConnected()) {
   logger.warn(
   "The HelixManager ({}-{}-{}) is still connected after {} ms wait. This is a potential resource leakage!",
   _clusterName, _instanceName, _type.name(), DISCONNECT_WAIT_TIME_MS);
   }
   }
   }*/
}

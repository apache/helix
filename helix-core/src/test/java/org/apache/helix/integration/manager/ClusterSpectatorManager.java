package org.apache.helix.integration.manager;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterSpectatorManager extends ClusterManager{
  private static Logger LOG = LoggerFactory.getLogger(ClusterControllerManager.class);

  private final CountDownLatch _startCountDown = new CountDownLatch(1);
  private final CountDownLatch _stopCountDown = new CountDownLatch(1);
  private final CountDownLatch _waitStopFinishCountDown = new CountDownLatch(1);

  private boolean _started = false;

  public ClusterSpectatorManager(String zkAddr, String clusterName) {
    this(zkAddr, clusterName, "spectator");
  }

  public ClusterSpectatorManager(String zkAddr, String clusterName, String spectatorName) {
    super(zkAddr, clusterName, spectatorName, InstanceType.SPECTATOR);
  }
}

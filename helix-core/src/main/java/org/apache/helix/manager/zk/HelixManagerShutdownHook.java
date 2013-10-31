package org.apache.helix.manager.zk;

import org.apache.helix.HelixManager;
import org.apache.log4j.Logger;

/**
 * Shutdown hook for helix manager
 * Working for kill -2/-15
 * NOT working for kill -9
 */
public class HelixManagerShutdownHook extends Thread {
  private static Logger LOG = Logger.getLogger(HelixManagerShutdownHook.class);

  final HelixManager _manager;

  public HelixManagerShutdownHook(HelixManager manager) {
    _manager = manager;
  }

  @Override
  public void run() {
    LOG.info("HelixControllerMainShutdownHook invoked on manager: " + _manager.getClusterName()
        + ", " + _manager.getInstanceName());
    _manager.disconnect();
  }
}

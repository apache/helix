package org.apache.helix.zookeeper.zkclient.callback;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkAsyncRetryThread extends Thread {
  private static Logger LOG = LoggerFactory.getLogger(ZkAsyncRetryThread.class);
  private BlockingQueue<ZkAsyncRetryCallContext> _retryContexts = new LinkedBlockingQueue<>();

  public ZkAsyncRetryThread(String name) {
    setDaemon(true);
    setName("ZkClient-AsyncCallback-Retry-" + getId() + "-" + name);
  }

  @Override
  public void run() {
    LOG.info("Starting ZkClient AsyncCallback retry thread.");
    try {
      while (!isInterrupted()) {
        ZkAsyncRetryCallContext context = _retryContexts.take();
        try {
          context.doRetry();
        } catch (Throwable e) {
          LOG.error("Error retrying callback " + context, e);
        }
      }
    } catch (InterruptedException e) {
      LOG.info("ZkClient AsyncCallback retry thread is interrupted.");
    }
    LOG.info("Terminate ZkClient AsyncCallback retry thread.");
  }

  boolean sendRetryRequest(ZkAsyncRetryCallContext context) {
    if (!isInterrupted()) {
      _retryContexts.add(context);
      return true;
    }
    return false;
  }
}

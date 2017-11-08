package org.apache.helix.common;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.helix.controller.stages.ClusterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic extended single-thread class to handle ClusterEvent (multiple-producer/single consumer
 * style).
 */
public abstract class ClusterEventProcessor extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(ClusterEventProcessor.class);

  protected final ClusterEventBlockingQueue _eventQueue;
  protected final String _clusterName;
  protected final String _processorName;

  public ClusterEventProcessor(String clusterName) {
    this(clusterName, "Helix-ClusterEventProcessor");
  }

  public ClusterEventProcessor(String clusterName, String processorName) {
    super(processorName + "-" + clusterName);
    _processorName = processorName;
    _eventQueue = new ClusterEventBlockingQueue();
    _clusterName = clusterName;
  }

  @Override
  public void run() {
    logger.info("START " + _processorName + " thread for cluster " + _clusterName);
    while (!isInterrupted()) {
      try {
        ClusterEvent event = _eventQueue.take();
        handleEvent(event);
      } catch (InterruptedException e) {
        logger.warn(_processorName + " thread interrupted", e);
        interrupt();
      } catch (ZkInterruptedException e) {
        logger.warn(_processorName + " thread caught a ZK connection interrupt", e);
        interrupt();
      } catch (ThreadDeath death) {
        throw death;
      } catch (Throwable t) {
        logger.error(_processorName + " thread failed while running the controller pipeline", t);
      }
    }
    logger.info("END " + _processorName + " thread");
  }

  protected abstract void handleEvent(ClusterEvent event);

  public void queueEvent(ClusterEvent event) {
    _eventQueue.put(event);
  }
}

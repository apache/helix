package org.apache.helix.common;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic extended single-thread class to handle event with events with the same type de-duplicated (multiple-producer/single consumer
 * style).
 *
 * T -- Type of the event.
 * E -- The event itself.
 */
public abstract class DedupEventProcessor<T, E> extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(DedupEventProcessor.class);

  protected final DedupEventBlockingQueue<T, E> _eventQueue;
  protected final String _clusterName;
  protected final String _processorName;

  public DedupEventProcessor(String processorName) {
    this(new String(), processorName);
  }

  public DedupEventProcessor(String clusterName, String processorName) {
    super(processorName + "-" + clusterName);
    _processorName = processorName;
    _eventQueue = new DedupEventBlockingQueue<>();
    _clusterName = clusterName;
  }

  public DedupEventProcessor() {
    this(new String(), "Default-DedupEventProcessor");
  }

  @Override
  public void run() {
    logger.info("START " + _processorName + " thread for cluster " + _clusterName);
    while (!isInterrupted()) {
      try {
        E event = _eventQueue.take();
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
        logger.error(_processorName + " thread failed while running " + _processorName, t);
      }
    }
    logger.info("END " + _processorName + " thread for cluster " + _clusterName);
  }

  protected abstract void handleEvent(E event);

  public void queueEvent(T eventType, E event) {
    if (isInterrupted()) {
      return;
    }
    _eventQueue.put(eventType, event);
  }

  public void resetEventQueue() {
    _eventQueue.clear();
  }

  public void shutdown() {
    this.interrupt();
    _eventQueue.clear();
  }
}

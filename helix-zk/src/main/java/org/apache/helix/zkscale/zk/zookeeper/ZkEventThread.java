package org.apache.helix.zkscale.zk.zookeeper;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkEventThread extends Thread {
  private static Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  private BlockingQueue<ZkEvent> _events = new LinkedBlockingQueue<>();

  private long _totalEventCount = 0L;
  private long _totalEventCountHandled = 0L;

  private static AtomicInteger _eventId = new AtomicInteger(0);

  public static abstract class ZkEvent {

    private String _description;

    public ZkEvent(String description) {
      _description = description;
    }

    public abstract void run() throws Exception;

    @Override public String toString() {
      return "ZkEvent[" + _description + "]";
    }
  }

  ZkEventThread(String name) {
    setDaemon(true);
    setName("ZkClient-EventThread-" + getId() + "-" + name);
  }

  @Override public void run() {
    LOG.info("Starting ZkClient event thread.");
    try {
      while (!isInterrupted()) {
        ZkEvent zkEvent = _events.take();
        int eventId = _eventId.incrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Delivering event #" + eventId + " " + zkEvent);
        }
        try {
          zkEvent.run();
          _totalEventCountHandled ++;
        } catch (InterruptedException e) {
          interrupt();
        } catch (ZkInterruptedException e) {
          interrupt();
        } catch (Throwable e) {
          LOG.error("Error handling event " + zkEvent, e);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Delivering event #" + eventId + " done");
        }
      }
    } catch (InterruptedException e) {
      LOG.info("Terminate ZkClient event thread.");
    }

    LOG.info("Terminate ZkClient event thread.");
  }

  public void send(ZkEvent event) {
    if (!isInterrupted()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("New event: " + event);
      }
      _events.add(event);
      _totalEventCount ++;
    }
  }

  public long getPendingEventsCount() {
    return _events.size();
  }

  public long getTotalEventCount() { return _totalEventCount; }

  public long getTotalHandledEventCount() { return _totalEventCountHandled; }
}

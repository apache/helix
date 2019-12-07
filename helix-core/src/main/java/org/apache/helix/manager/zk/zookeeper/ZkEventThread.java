/**
 * Copyright 2010 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.helix.manager.zk.zookeeper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All listeners registered at the {@link ZkClient} will be notified from this event thread. This is to prevent
 * dead-lock situations. The {@link ZkClient} pulls some information out of the {@link ZooKeeper} events to signal
 * {@link ZkLock} conditions. Re-using the {@link ZooKeeper} event thread to also notify {@link ZkClient} listeners,
 * would stop the ZkClient from receiving events from {@link ZooKeeper} as soon as one of the listeners blocks (because
 * it is waiting for something). {@link ZkClient} would then for instance not be able to maintain it's connection state
 * anymore.
 */
public class ZkEventThread extends Thread {
  private static Logger LOG = LoggerFactory.getLogger(ZkClient.class);

  private BlockingQueue<ZkEvent> _events = new LinkedBlockingQueue<>();

  private long _totalEventCount = 0L;
  private long _totalEventCountHandled = 0L;

  private static AtomicInteger _eventId = new AtomicInteger(0);

  public static abstract class ZkEvent {

    private final String _description;
    private final String _sessionId;

    public ZkEvent(String description) {
      this(description, null);
    }

    ZkEvent(String description, String sessionId) {
      _description = description;
      _sessionId = sessionId;
    }

    public abstract void run() throws Exception;

    @Override
    public String toString() {
      return "ZkEvent[description: " + _description + "; session: " + _sessionId + "]";
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

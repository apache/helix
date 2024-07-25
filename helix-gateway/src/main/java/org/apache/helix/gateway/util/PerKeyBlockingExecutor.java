package org.apache.helix.gateway.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A per-key blocking executor that ensures that only one event is running for a given key at a time.
 */
public class PerKeyBlockingExecutor {
  private final ThreadPoolExecutor _executor;
  private final Map<String, Queue<Runnable>> _pendingBlockedEvents;
  private final ConcurrentHashMap.KeySetView<String, Boolean> _runningEvents;
  private final Lock _queueLock;

  public PerKeyBlockingExecutor(int maxWorkers) {
    this._executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxWorkers);
    this._pendingBlockedEvents = new HashMap<>();
    this._queueLock = new ReentrantLock();
    this._runningEvents = ConcurrentHashMap.newKeySet();
  }

  /**
   * Offer an event to be executed. If an event is already running for the given key, the event will be queued.
   * @param key
   * @param event
   */
  public void offerEvent(String key, Runnable event) {
    _queueLock.lock();
    try {
      if (!_runningEvents.contains(key)) {
        _executor.execute(() -> runEvent(key, event));
      } else {
        _pendingBlockedEvents.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>());
        _pendingBlockedEvents.get(key).offer(event);
      }
    } finally {
      _queueLock.unlock();
    }
  }

  private void runEvent(String key, Runnable event) {
    try {
      _runningEvents.add(key);
      event.run();
    } finally {
      _queueLock.lock();
      try {
        _runningEvents.remove(key);
        processQueue(key);
      } finally {
        _queueLock.unlock();
      }
    }
  }

  private void processQueue(String key) {
    if (!_pendingBlockedEvents.containsKey(key)) {
      return;
    }
    Runnable event = _pendingBlockedEvents.get(key).poll();
    if (event != null) {
      _executor.execute(() -> runEvent(key, event));
    }
  }

  public void shutdown() {
    _executor.shutdown();
  }

}

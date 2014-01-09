package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A blocking queue of ClusterEvent objects to be used by the controller pipeline. This prevents
 * multiple events of the same type from flooding the controller and preventing progress from being
 * made. This queue has no capacity. This class is meant to be a limited implementation of the
 * {@link BlockingQueue} interface.
 */
public class ClusterEventBlockingQueue {
  private static final Logger LOG = Logger.getLogger(ClusterEventBlockingQueue.class);
  private final Map<String, ClusterEvent> _eventMap;
  private final Queue<ClusterEvent> _eventQueue;

  /**
   * Instantiate the queue
   */
  public ClusterEventBlockingQueue() {
    _eventMap = Maps.newHashMap();
    _eventQueue = Lists.newLinkedList();
  }

  /**
   * Remove all events from the queue
   */
  public synchronized void clear() {
    _eventMap.clear();
    _eventQueue.clear();
  }

  /**
   * Add a single event to the queue, overwriting events with the same name
   * @param event ClusterEvent event to add
   */
  public synchronized void put(ClusterEvent event) {
    if (!_eventMap.containsKey(event.getName())) {
      // only insert if there isn't a same-named event already present
      boolean result = _eventQueue.offer(event);
      if (!result) {
        return;
      }
    }
    // always overwrite in case this is a FINALIZE
    _eventMap.put(event.getName(), event);
    LOG.debug("Putting event " + event.getName());
    LOG.debug("Event queue size: " + _eventQueue.size());
    notify();
  }

  /**
   * Remove an element from the front of the queue, blocking if none is available. This method
   * will return the most recent event seen with the oldest enqueued event name.
   * @return ClusterEvent at the front of the queue
   * @throws InterruptedException if the wait for elements was interrupted
   */
  public synchronized ClusterEvent take() throws InterruptedException {
    while (_eventQueue.isEmpty()) {
      wait();
    }
    ClusterEvent queuedEvent = _eventQueue.poll();
    if (queuedEvent != null) {
      LOG.debug("Taking event " + queuedEvent.getName());
      LOG.debug("Event queue size: " + _eventQueue.size());
      return _eventMap.remove(queuedEvent.getName());
    }
    return null;
  }

  /**
   * Get at the head of the queue without removing it
   * @return ClusterEvent at the front of the queue, or null if none available
   */
  public synchronized ClusterEvent peek() {
    ClusterEvent queuedEvent = _eventQueue.peek();
    if (queuedEvent != null) {
      return _eventMap.get(queuedEvent.getName());
    }
    return queuedEvent;
  }

  /**
   * Get the queue size
   * @return integer size of the queue
   */
  public int size() {
    return _eventQueue.size();
  }

  /**
   * Check if the queue is empty
   * @return true if events are not present, false otherwise
   */
  public boolean isEmpty() {
    return _eventQueue.isEmpty();
  }
}

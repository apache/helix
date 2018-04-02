package org.apache.helix.common;

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
import java.util.concurrent.BlockingQueue;

import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A blocking queue of ClusterEvent objects to be used by the controller pipeline. This prevents
 * multiple events of the same type from flooding the controller and preventing progress from being
 * made. This queue has no capacity. This class is meant to be a limited implementation of the
 * {@link BlockingQueue} interface.
 *
 * This class is deprecated, please use {@link org.apache.helix.common.DedupEventBlockingQueue}.
 */
@Deprecated
public class ClusterEventBlockingQueue {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterEventBlockingQueue.class);

  private DedupEventBlockingQueue<ClusterEventType, ClusterEvent> _eventQueue;

  /**
   * Instantiate the queue
   */
  public ClusterEventBlockingQueue() {
    _eventQueue = new DedupEventBlockingQueue();
  }

  /**
   * Remove all events from the queue
   */
  public void clear() {
    _eventQueue.clear();
  }

  /**
   * Add a single event to the queue, overwriting events with the same name
   * @param event ClusterEvent event to add
   */
  public void put(ClusterEvent event) {
    _eventQueue.put(event.getEventType(), event);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Putting event " + event.getEventType());
      LOG.debug("Event queue size: " + _eventQueue.size());
    }
  }

  /**
   * Remove an element from the front of the queue, blocking if none is available. This method
   * will return the most recent event seen with the oldest enqueued event name.
   * @return ClusterEvent at the front of the queue
   * @throws InterruptedException if the wait for elements was interrupted
   */
  public ClusterEvent take() throws InterruptedException {
    ClusterEvent event = _eventQueue.take();
    if (event != null) {
      LOG.debug("Taking event " + event.getEventType());
      LOG.debug("Event queue size: " + _eventQueue.size());
    }
    return event;
  }

  /**
   * Get at the head of the queue without removing it
   * @return ClusterEvent at the front of the queue, or null if none available
   */
  public ClusterEvent peek() {
    return _eventQueue.peek();
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

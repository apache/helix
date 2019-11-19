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

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A blocking queue of events, which automatically deduplicate events with the same "type" within
 * the queue, i.e, when putting an event into the queue, if there is already an event with the
 * same type existing in the queue, the new event won't be inserted into the queue.
 * This class is meant to be a limited implementation of the {@link BlockingQueue} interface.
 *
 * T -- the Type of an event.
 * E -- the event itself.
 */
public class DedupEventBlockingQueue<T, E> {
  private final Map<T, Entry<T, E>> _eventMap;
  private final Queue<Entry> _eventQueue;

  class Entry <T, E> {
    private T _type;
    private E _event;

    Entry (T type, E event) {
      _type = type;
      _event = event;
    }

    T getType() {
      return _type;
    }

    E getEvent() {
      return _event;
    }
  }

  /**
   * Instantiate the queue
   */
  public DedupEventBlockingQueue() {
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
   */
  public synchronized void put(T type, E event) {
    Entry entry = new Entry(type, event);

    if (!_eventMap.containsKey(entry.getType())) {
      // only insert to the queue if there isn't a same-typed event already present
      boolean result = _eventQueue.offer(entry);
      if (!result) {
        return;
      }
    }
    // always overwrite the existing entry in the map in case the entry is different
    _eventMap.put((T) entry.getType(), entry);
    notify();
  }

  /**
   * Remove an element from the front of the queue, blocking if none is available. This method
   * will return the most recent event seen with the oldest enqueued event name.
   * @return ClusterEvent at the front of the queue
   * @throws InterruptedException if the wait for elements was interrupted
   */
  public synchronized E take() throws InterruptedException {
    while (_eventQueue.isEmpty()) {
      wait();
    }
    Entry entry = _eventQueue.poll();
    if (entry != null) {
      entry = _eventMap.remove(entry.getType());
      return (E) entry.getEvent();
    }
    return null;
  }

  /**
   * Get at the head of the queue without removing it
   * @return ClusterEvent at the front of the queue, or null if none available
   */
  public synchronized E peek() {
    Entry entry = _eventQueue.peek();
    if (entry != null) {
      entry = _eventMap.get(entry.getType());
      return (E) entry.getEvent();
    }
    return null;
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

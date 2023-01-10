package org.apache.helix.zookeeper.zkclient;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;


/**
 * A thread-safe container wrapper class for Listeners registered to ZkClient.
 * It stores 2 types of listener separately, one-time listener and persistent listener. The former ones are removed
 * right after its consumption, while the latter need to be explicitly removed. 
 * @param <T> the type of listener
 */
public class ListenerContainer<T> {
  private final ReentrantLock _lock = new ReentrantLock(true);
  private final Map<String, Set<T>> _persistentListener = new ConcurrentHashMap<>();
  private final Map<String, Set<T>> _onetimeListener = new ConcurrentHashMap<>();

  /**
   * Add listener to the container with specified key.
   * @param key the key to register to
   * @param listener the listener to register
   * @param persistListener true if the listener is persistent
   */
  public void addListener(String key, T listener, boolean persistListener) {
    addListener(key, listener, persistListener ? _persistentListener : _onetimeListener);
  }

  /**
   * Remove listener from the container.
   * This operation removes both one-time and persistent listener from the specified key.
   * @param key the key to remove
   * @param listener the listener to remove
   */
  public void removeListener(String key, T listener) {
    _lock.lock();
    try {
      removeFromListenerMap(key, listener, _persistentListener);
      removeFromListenerMap(key, listener, _onetimeListener);
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Remove listener from the container.
   * @param key the key to remove
   * @param listener the listener to remove
   * @param persistListener true if remove the persistent listener, otherwise remove the one-time listener
   */
  public void removeListener(String key, T listener, boolean persistListener) {
    _lock.lock();
    try {
      if (persistListener) {
        removeFromListenerMap(key, listener, _persistentListener);
      } else {
        removeFromListenerMap(key, listener, _onetimeListener);
      }
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Remove listeners from the container.
   * @param key the key to remove
   * @param listeners the listeners to remove
   * @param persistListener true if remove the persistent listener, otherwise remove the one-time listener
   */
  public void removeListeners(String key, Collection<T> listeners, boolean persistListener) {
    _lock.lock();
    try {
      if (persistListener) {
        removeFromListenerMap(key, listeners, _persistentListener);
      } else {
        removeFromListenerMap(key, listeners, _onetimeListener);
      }
    } finally {
      _lock.unlock();
    }
  }

  /**
   * Consume the listeners registered to the given key. The given consumer is applied to all one-time and persistent
   * listeners with the given key.
   * The one-time listeners are removed after this operation, as a result, this operation is NOT idempotent.
   * @param key the key of the listeners
   * @param consumer the consuming action for the listeners
   */
  public void consumeListeners(String key, Consumer<? super T> consumer) {
    Set<T> onetimeListeners;
    Set<T> persistentListeners;
    _lock.lock();
    try {
      // remove one-time listeners
      onetimeListeners = _onetimeListener.remove(key);
    } finally {
      _lock.unlock();
    }
    if (onetimeListeners == null) {
      onetimeListeners = Collections.emptySet();
    }
    persistentListeners = _persistentListener.getOrDefault(key, Collections.emptySet());
    if (persistentListeners.isEmpty() && onetimeListeners.isEmpty()) {
      return;
    }
    Set<T> listeners = new HashSet<>(onetimeListeners);
    listeners.addAll(persistentListeners);
    listeners.forEach(consumer);
  }

  public boolean isEmpty(String key) {
    return !_persistentListener.containsKey(key) && !_onetimeListener.containsKey(key);
  }

  private void addListener(String key, T listener, Map<String, Set<T>> listenerMap) {
    _lock.lock();
    try {
      Set<T> entryListeners = listenerMap.computeIfAbsent(key, k -> new CopyOnWriteArraySet<>());
      entryListeners.add(listener);
    } finally {
      _lock.unlock();
    }
  }

  private static <T> void removeFromListenerMap(String key, T listener, Map<String, Set<T>> listenerMap) {
    Set<T> listeners = listenerMap.get(key);
    if (listeners == null) {
      return;
    }
    listeners.remove(listener);
    if (listeners.isEmpty()) {
      listenerMap.remove(key);
    }
  }

  private static <T> void removeFromListenerMap(String key, Collection<T> listeners, Map<String, Set<T>> listenerMap) {
    if (listeners == null || listeners.isEmpty()) {
      return;
    }
    Set<T> current = listenerMap.get(key);
    if (current == null || current.isEmpty()) {
      return;
    }
    current.removeAll(listeners);
    if (current.isEmpty()) {
      listenerMap.remove(key);
    }
  }

  @VisibleForTesting
  Map<String, Set<T>> getPersistentListener() {
    return _persistentListener;
  }

  @VisibleForTesting
  Map<String, Set<T>> getOnetimeListener() {
    return _onetimeListener;
  }
}

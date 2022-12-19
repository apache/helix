package org.apache.helix.metaclient.impl.common;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * A container wrapper class for Listeners registered to {@link org.apache.helix.metaclient.api.MetaClientInterface}
 * @param <T> the type of listener
 */
public class ListenerContainer<T> {
  private final ReentrantReadWriteLock _readWriteLock = new ReentrantReadWriteLock(true);
  private final Map<String, Set<T>> _persistentListener = new HashMap<>();
  private final Map<String, Set<T>> _onetimeListener = new HashMap<>();

  public void addListener(String key, T listener, boolean persistListener) {
    _readWriteLock.writeLock().lock();
    try {
      if (persistListener) {
        Set<T> entryListeners = _persistentListener.computeIfAbsent(key, k -> new HashSet<>());
        entryListeners.add(listener);
        _persistentListener.put(key, entryListeners);
      } else {
        Set<T> entryListeners = _onetimeListener.computeIfAbsent(key, k -> new HashSet<>());
        entryListeners.add(listener);
        _onetimeListener.put(key, entryListeners);
      }
    } finally {
      _readWriteLock.writeLock().unlock();
    }
  }

  public void removeListener(String key, T listener) {
    _readWriteLock.writeLock().lock();
    try {
      removeFromListenerMap(key, listener, _persistentListener);
      removeFromListenerMap(key, listener, _onetimeListener);
    } finally {
      _readWriteLock.writeLock().unlock();
    }
  }

  public void removeListener(String key, T listener, boolean persistListener) {
    _readWriteLock.writeLock().lock();
    try {
      if (persistListener) {
        removeFromListenerMap(key, listener, _persistentListener);
      } else {
        removeFromListenerMap(key, listener, _onetimeListener);
      }
    } finally {
      _readWriteLock.writeLock().unlock();
    }
  }

  public void removeListeners(String key, Set<T> listeners, boolean persistListener) {
    _readWriteLock.writeLock().lock();
    try {
      if (persistListener) {
        removeFromListenerMap(key, listeners, _persistentListener);
      } else {
        removeFromListenerMap(key, listeners, _onetimeListener);
      }
    } finally {
      _readWriteLock.writeLock().unlock();
    }
  }

  /**
   * Consume the listeners registered to the given key. The given consumer is applied to all one-time and persistent
   * listeners with the given key.
   * The one-time listeners are removed after this operation.
   * @param key the key of the listeners
   * @param consumer the consuming action for the listeners
   */
  public void consumeListeners(String key, Consumer<? super T> consumer) {
    Set<T> onetimeListeners;
    Set<T> persistentListeners;
    _readWriteLock.readLock().lock();
    try {
      persistentListeners = _persistentListener.getOrDefault(key, Collections.emptySet());
      onetimeListeners = _onetimeListener.getOrDefault(key, Collections.emptySet());
      if (persistentListeners.isEmpty() && onetimeListeners.isEmpty()) {
        return;
      }
    } finally {
      _readWriteLock.readLock().unlock();
    }
    Stream.concat(persistentListeners.stream(), onetimeListeners.stream()).forEach(consumer);
    // remove one-time listeners
    if (!onetimeListeners.isEmpty()) {
      removeListeners(key, onetimeListeners, false);
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

  private static <T> void removeFromListenerMap(String key, Set<T> listeners, Map<String, Set<T>> listenerMap) {
    if (listeners == null || listeners.isEmpty()) {
      return;
    }
    Set<T> current = listenerMap.get(key);
    if (current == null || current.isEmpty()) {
      return;
    }
    current.removeAll(listeners);
  }
}

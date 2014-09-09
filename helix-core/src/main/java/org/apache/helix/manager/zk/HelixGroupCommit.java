package org.apache.helix.manager.zk;

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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class HelixGroupCommit<T> {
  private static Logger LOG = Logger.getLogger(HelixGroupCommit.class);

  private static class Queue<T> {
    final AtomicReference<Thread> _running = new AtomicReference<Thread>();
    final ConcurrentLinkedQueue<Entry<T>> _pending = new ConcurrentLinkedQueue<Entry<T>>();
  }

  private static class Entry<T> {
    final String _key;
    final DataUpdater<T> _updater;
    AtomicBoolean _sent = new AtomicBoolean(false);
    boolean _isSuccess;

    Entry(String key, DataUpdater<T> updater) {
      _key = key;
      _updater = updater;
      _isSuccess = false;
    }
  }

  private final Queue<T>[] _queues = new Queue[100];

  public HelixGroupCommit() {
    // Don't use Arrays.fill();
    for (int i = 0; i < _queues.length; ++i) {
      _queues[i] = new Queue<T>();
    }
  }

  private Queue<T> getQueue(String key) {
    return _queues[(key.hashCode() & Integer.MAX_VALUE) % _queues.length];
  }

  public boolean commit(ZkBaseDataAccessor<T> accessor, int options, String key,
      DataUpdater<T> updater) {
    Queue<T> queue = getQueue(key);
    Entry<T> entry = new Entry<T>(key, updater);

    queue._pending.add(entry);

    boolean success = false;
    while (!entry._sent.get()) {
      if (queue._running.compareAndSet(null, Thread.currentThread())) {
        ArrayList<Entry<T>> processed = new ArrayList<Entry<T>>();
        try {
          Entry<T> first = queue._pending.peek();
          if (first == null) {
            return true;
          }

          String mergedKey = first._key;

          boolean retry;
          do {
            retry = false;

            try {
              T merged = null;

              Stat readStat = new Stat();

              // to create a new znode, we need set version to -1
              readStat.setVersion(-1);
              try {
                // accessor will fallback to zk if not found in cache
                merged = accessor.get(mergedKey, readStat, options);
              } catch (ZkNoNodeException e) {
                // OK
              }

              // iterate over processed if we are retrying
              Iterator<Entry<T>> it = processed.iterator();
              while (it.hasNext()) {
                Entry<T> ent = it.next();
                if (!ent._key.equals(mergedKey)) {
                  continue;
                }
                merged = ent._updater.update(merged);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("After merging processed entry. path:" + mergedKey + ", value: "
                      + merged);
                }
              }

              // iterate over queue._pending for newly coming requests
              it = queue._pending.iterator();
              while (it.hasNext()) {
                Entry<T> ent = it.next();
                if (!ent._key.equals(mergedKey)) {
                  continue;
                }
                processed.add(ent);
                merged = ent._updater.update(merged);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("After merging pending entry. path:" + mergedKey + ", value: " + merged);
                }

                it.remove();
              }
              success = accessor.set(mergedKey, merged, readStat.getVersion(), options);
              if (!success) {
                LOG.error("Fail to group commit. path: " + mergedKey + ", value: " + merged
                    + ", version: " + readStat.getVersion());
              }
            } catch (ZkBadVersionException e) {
              retry = true;
            }
          } while (retry);
        } finally {
          queue._running.set(null);
          for (Entry<T> e : processed) {
            synchronized (e) {
              e._sent.set(true);
              e._isSuccess = success;
              e.notify();
            }
          }
        }
      } else {
        synchronized (entry) {
          try {
            entry.wait(10);
          } catch (InterruptedException e) {
            LOG.error("Interruped while committing change, key: " + key, e);
            Thread.currentThread().interrupt();
            return false;
          }
        }
      }
    }
    return entry._isSuccess;
  }
}

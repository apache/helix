package org.apache.helix;

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

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: move to mananger.zk
/**
 * Support committing updates to data such that they are ordered for each key
 */
public class GroupCommit {
  private static Logger LOG = LoggerFactory.getLogger(GroupCommit.class);
  private static int MAX_RETRY = 3;

  private static class Queue {
    final AtomicReference<Thread> _running = new AtomicReference<Thread>();
    final ConcurrentLinkedQueue<Entry> _pending = new ConcurrentLinkedQueue<Entry>();
  }

  private static class Entry {
    final String _key;
    final ZNRecord _record;
    AtomicBoolean _sent = new AtomicBoolean(false);

    Entry(String key, ZNRecord record) {
      _key = key;
      _record = record;
    }
  }

  private final Queue[] _queues = new Queue[100];

  /**
   * Set up a group committer and its associated queues
   */
  public GroupCommit() {
    // Don't use Arrays.fill();
    for (int i = 0; i < _queues.length; ++i) {
      _queues[i] = new Queue();
    }
  }

  private Queue getQueue(String key) {
    return _queues[(key.hashCode() & Integer.MAX_VALUE) % _queues.length];
  }

  /**
   * Do a group update for data associated with a given key
   * @param accessor accessor with the ability to pull from the current data
   * @param options see {@link AccessOption}
   * @param key the data identifier
   * @param record the data to be merged in
   * @return true if successful, false otherwise
   */
  public boolean commit(BaseDataAccessor<ZNRecord> accessor, int options, String key,
      ZNRecord record) {
    return commit(accessor, options, key, record, false);
  }

  public boolean commit(BaseDataAccessor<ZNRecord> accessor, int options, String key,
      ZNRecord record, boolean removeIfEmpty) {
    Queue queue = getQueue(key);
    Entry entry = new Entry(key, record);

    boolean success = true;
    queue._pending.add(entry);

    while (!entry._sent.get()) {
      if (queue._running.compareAndSet(null, Thread.currentThread())) {
        ArrayList<Entry> processed = new ArrayList<>();
        try {
          if (queue._pending.peek() == null)
            return true;

          // remove from queue
          Entry first = queue._pending.poll();
          processed.add(first);

          String mergedKey = first._key;
          ZNRecord merged = null;

          try {
            // accessor will fallback to zk if not found in cache
            merged = accessor.get(mergedKey, null, options);
          } catch (ZkNoNodeException e) {
            // OK.
          }

          /**
           * If the local cache does not contain a value, need to check if there is a
           * value in ZK; use it as initial value if exists
           */
          if (merged == null) {
            merged = new ZNRecord(first._record);
          } else {
            merged.merge(first._record);
          }
          Iterator<Entry> it = queue._pending.iterator();
          while (it.hasNext()) {
            Entry ent = it.next();
            if (!ent._key.equals(mergedKey))
              continue;
            processed.add(ent);
            merged.merge(ent._record);
            // System.out.println("After merging:" + merged);
            it.remove();
          }

          int retry = 0;
          success = false;
          while (++retry <= MAX_RETRY && !success) {
            if (removeIfEmpty && merged.getMapFields().isEmpty()) {
              success = accessor.remove(mergedKey, options);
              if (!success) {
                LOG.error("Fails to remove " + mergedKey + " from ZK, retry it!");
              }
            } else {
              success = accessor.set(mergedKey, merged, options);
              if (!success) {
                LOG.error("Fails to update " + mergedKey + " to ZK, retry it! ");
              }
            }
          }
        } finally {
          queue._running.set(null);
          for (Entry e : processed) {
            synchronized (e) {
              e._sent.set(true);
              e.notify();
            }
          }
        }
      } else {
        synchronized (entry) {
          try {
            entry.wait(10);
          } catch (InterruptedException e) {
            LOG.error("Interrupted while committing change, key: " + key + ", record: " + record, e);
            // Restore interrupt status
            Thread.currentThread().interrupt();
            return false;
          }
        }
      }
    }
    return success;
  }

}

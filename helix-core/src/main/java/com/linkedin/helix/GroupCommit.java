package com.linkedin.helix;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

public class GroupCommit
{ 
  private static Logger  LOG = Logger.getLogger(GroupCommit.class);
  private static class Queue
  {
    final AtomicReference<Thread>      _running = new AtomicReference<Thread>();
    final ConcurrentLinkedQueue<Entry> _pending = new ConcurrentLinkedQueue<Entry>();
  }

  private static class Entry
  {
    final String   _key;
    final ZNRecord _record;
    AtomicBoolean  _sent = new AtomicBoolean(false);

    Entry(String key, ZNRecord record)
    {
      _key = key;
      _record = record;
    }
  }

  private final Queue[]               _queues = new Queue[100];
  
  // potential memory leak if we add resource and remove resource
  // TODO: move the cache logic to data accessor
  private final Map<String, ZNRecord> _cache  = new ConcurrentHashMap<String, ZNRecord>();

  
  public GroupCommit()
  {
    // Don't use Arrays.fill();
    for (int i = 0; i < _queues.length; ++i)
    {
      _queues[i] = new Queue();
    }
  }

  private Queue getQueue(String key)
  {
    return _queues[(key.hashCode() & Integer.MAX_VALUE) % _queues.length];
  }

  public boolean commit(BaseDataAccessor<ZNRecord> accessor, String key, ZNRecord record)
  {
    Queue queue = getQueue(key);
    Entry entry = new Entry(key, record);

    queue._pending.add(entry);

    while (!entry._sent.get())
    {
      if (queue._running.compareAndSet(null, Thread.currentThread()))
      {
        ArrayList<Entry> processed = new ArrayList<Entry>();
        try
        {
          if (queue._pending.peek() == null)
            return true;

          // remove from queue
          Entry first = queue._pending.poll();
          processed.add(first);

          String mergedKey = first._key;
          ZNRecord merged = _cache.get(mergedKey);
          /**
           * If the local cache does not contain a value, need to check if there is a 
           * value in ZK; use it as initial value if exists
           */
          if (merged == null)
          {
            ZNRecord valueOnZk = null;
            try
            {
              valueOnZk = accessor.get(mergedKey, null, 0);
            }
            catch(Exception e)
            {
              LOG.info(e);
            }
            if(valueOnZk != null)
            {
              merged = valueOnZk;
              merged.merge(first._record);
            }
            else // Zk path has null data. use the first record as initial record.
            {
              merged = new ZNRecord(first._record);
            }
          }
          else
          {
            merged.merge(first._record);
          }
          Iterator<Entry> it = queue._pending.iterator();
          while (it.hasNext())
          {
            Entry ent = it.next();
            if (!ent._key.equals(mergedKey))
              continue;
            processed.add(ent);
            merged.merge(ent._record);
            // System.out.println("After merging:" + merged);
            it.remove();
          }
          // System.out.println("size:"+ processed.size());
          accessor.set(mergedKey, merged, BaseDataAccessor.Option.PERSISTENT);
          _cache.put(mergedKey, merged);
        }
        finally
        {
          queue._running.set(null);
          for (Entry e : processed)
          {
            synchronized (e)
            {
              e._sent.set(true);
              e.notify();
            }
          }
        }
      }
      else
      {
        synchronized (entry)
        {
          try
          {
            entry.wait(10);
          }
          catch (InterruptedException e)
          {
            e.printStackTrace();
            return false;
          }
        }
      }
    }
    return true;
  }

}

package com.linkedin.helix.store.zk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.PropertyChangeListener;
import com.linkedin.helix.store.PropertySerializer;
import com.linkedin.helix.store.PropertyStat;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.PropertyStoreException;
import com.linkedin.helix.store.zk.PropertyItem.ByteArray;

public class ZKPropertyStore<T> implements PropertyStore<T>, IZkStateListener // IZkDataListener, IZkChildListener,
{
  private static Logger LOG = Logger.getLogger(ZKPropertyStore.class);

  static class ByteArraySerializer implements ZkSerializer
  {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError
    {
      if (data == null)
      {
        return null;
      } else
      {
        return ( (ByteArray)data)._bytes;
      }
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError
    {
      if (bytes == null)
      {
        return null;
      } else
      {
        return new ByteArray(bytes);
      }
    }

  };

  class ByteArrayUpdater implements DataUpdater<ByteArray>
  {
    final DataUpdater<T> _updater;
    final PropertySerializer<T> _serializer;

    ByteArrayUpdater(DataUpdater<T> updater, PropertySerializer<T> serializer)
    {
      _updater = updater;
      _serializer = serializer;
    }

    @Override
    public ByteArray update(ByteArray current)
    {
      try
      {
        T currentValue = null;
        if (current != null)
        {
          currentValue = _serializer.deserialize(current.getBytes());
        }
        T updateValue = _updater.update(currentValue);
        return new ByteArray(_serializer.serialize(updateValue));
      } catch (PropertyStoreException e)
      {
        LOG.error("Exception in update. Updater: " + _updater, e);
      }
      return null;
    }
  }

  private volatile boolean _isConnected = false;
  private volatile boolean _hasSessionExpired = false;

  protected final ZkClient _zkClient;
  protected PropertySerializer<T> _serializer;
  protected final String _root;

  // zookeeperPath->userCallbak->zkCallback
  private final Map<String, Map<PropertyChangeListener<T>, ZkCallbackHandler<T>>> _callbackMap
      = new HashMap<String, Map<PropertyChangeListener<T>, ZkCallbackHandler<T>>>();

  // TODO cache capacity should be bounded
  private final Map<String, PropertyItem> _cache = new ConcurrentHashMap<String, PropertyItem>();

  public ZKPropertyStore(ZkClient zkClient,
      final PropertySerializer<T> serializer, String root)
  {
    if (zkClient == null || serializer == null || root == null)
    {
      throw new IllegalArgumentException("zkClient|serializer|root can't be null");
    }

    _root = normalizeKey(root);
    _zkClient = zkClient;
    _zkClient.setZkSerializer(new ByteArraySerializer());

    setPropertySerializer(serializer);

    _zkClient.createPersistent(_root, true);
    _zkClient.subscribeStateChanges(this);
  }

  // key is normalized if it has exactly 1 leading slash
  private String normalizeKey(String key)
  {
    if (key == null)
    {
      LOG.error("Key can't be null");
      throw new IllegalArgumentException("Key can't be null");
    }

    // strip off leading slash
    while (key.startsWith("/"))
    {
      key = key.substring(1);
    }

    return "/" + key;
  }

  private String getAbsolutePath(String key)
  {
    key = normalizeKey(key);
    if (key.equals("/"))
    {
      return _root;
    } else
    {
      return _root + key;
    }
  }

  // always a return normalized key
  String getRelativePath(String path)
  {
    if (!path.startsWith(_root))
    {
      String errMsg = path + "does NOT start with property store's root: " + _root;
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }

    if (path.equals(_root))
    {
      return "/";
    } else
    {
      return path.substring(_root.length());
    }
  }

  @Override
  public void createPropertyNamespace(String prefix) throws PropertyStoreException
  {
    String path = getAbsolutePath(prefix);
    try
    {
      if (!_zkClient.exists(path))
      {
        _zkClient.createPersistent(path, true);
      }
    } catch (Exception e)
    {
      LOG.error("Exception in creatPropertyNamespace(" + prefix + ")", e);
      throw new PropertyStoreException(e.toString());
    }
  }

  @Override
  public void setProperty(String key, final T value)
      throws PropertyStoreException
  {
    String path = getAbsolutePath(key);

    try
    {
      if (!_zkClient.exists(path))
      {
        _zkClient.createPersistent(path, true);
      }

      // serializer should handle value == null
      byte[] valueBytes = _serializer.serialize(value);
      _zkClient.writeData(path, new ByteArray(valueBytes));

      // update cache
      // getProperty(key);

    } catch (Exception e)
    {
      LOG.error("Exception when setProperty(" + key + ", " + value + ")", e);
      throw new PropertyStoreException(e.toString());
    }
  }

  @Override
  public T getProperty(String key) throws PropertyStoreException
  {
    return getProperty(key, null);
  }

  // bytes and stat are not null
  private T getValueAndStat(byte[] bytes, Stat stat, PropertyStat propertyStat)
    throws PropertyStoreException
  {
    T value = _serializer.deserialize(bytes);

    if (propertyStat != null)
    {
      propertyStat.setLastModifiedTime(stat.getMtime());
      propertyStat.setVersion(stat.getVersion());
    }
    return value;
  }

  @Override
  public T getProperty(String key, PropertyStat propertyStat)
      throws PropertyStoreException
  {
    String normalizedKey = normalizeKey(key);
    String path = getAbsolutePath(normalizedKey);
    Stat stat = new Stat();

    T value = null;
    try
    {
      if (_cache.containsKey(normalizedKey))
      {
        // cache hit
        stat = _zkClient.getStat(path);
        if (stat != null)
        {
          PropertyItem item = _cache.get(normalizedKey);
          if (item.getVersion() < stat.getVersion())
          {
            // stale data in cache
            ByteArray bytes = _zkClient.readDataAndStat(path, stat, true);
            if (bytes != null)
            {
              value = getValueAndStat(bytes.getBytes(), stat, propertyStat);
              _cache.put(normalizedKey, new PropertyItem(bytes.getBytes(), stat));
            } else
            {
              _cache.remove(normalizedKey);
            }

          } else
          {
            // valid data in cache
            // item.getBytes() should not be null
            value = getValueAndStat(item.getBytes(), stat, propertyStat);
          }
        } else
        {
          // stat == null means the znode doesn't exist
          _cache.remove(normalizedKey);
        }
      } else
      {
        // cache miss
        ByteArray bytes = _zkClient.readDataAndStat(path, stat, true);
        if (bytes != null)
        {
          value = getValueAndStat(bytes.getBytes(), stat, propertyStat);
          _cache.put(normalizedKey, new PropertyItem(bytes.getBytes(), stat));
        }
      }

      return value;
    } catch (Exception e)
    {
      LOG.error("Exception in getProperty(" + key + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }
  }

  @Override
  public void removeProperty(String key) throws PropertyStoreException
  {
    String normalizedKey = normalizeKey(key);
    String path = getAbsolutePath(normalizedKey);

    try
    {
      if (_zkClient.exists(path))
      {
        _zkClient.delete(path);
      }
      _cache.remove(normalizedKey);

    } catch (Exception e)
    {
      LOG.error("Exception in removeProperty(" + key + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }
  }

  @Override
  public String getPropertyRootNamespace()
  {
    return _root;
  }

  @Override
  public void removeNamespace(String prefix) throws PropertyStoreException
  {
    String path = getAbsolutePath(prefix);

    try
    {
      if (_zkClient.exists(path))
      {
        _zkClient.deleteRecursive(path);
      }

      // update cache
      // childs are all normalized keys
      List<String> childs = getPropertyNames(prefix);
      for (String child : childs)
      {
        _cache.remove(child);
      }
    } catch (Exception e)
    {
      LOG.error("Exception in removeProperty(" + prefix + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }
  }

  // prefix is always normalized
  private void doGetPropertyNames(String prefix, List<String> leafNodes)
    throws PropertyStoreException
  {
    String path = getAbsolutePath(prefix);

    if (!_zkClient.exists(path))
    {
      return;
    }

    List<String> childs = _zkClient.getChildren(path);
    if (childs == null)
    {
      return;
    }

    if (childs.size() == 0)
    {
      // add leaf node to cache
//      getProperty(prefix);
      leafNodes.add(prefix);
      return;
    }

    for (String child : childs)
    {
      String childPath = prefix.equals("/") ? prefix+child : prefix + "/" + child;
      doGetPropertyNames(childPath, leafNodes);
    }
  }

  @Override
  public List<String> getPropertyNames(String prefix)
      throws PropertyStoreException
  {
    String normalizedKey = normalizeKey(prefix);
    List<String> propertyNames = new ArrayList<String>();
    doGetPropertyNames(normalizedKey, propertyNames);

    // sort it to get deterministic order
    if (propertyNames.size() > 1)
    {
      Collections.sort(propertyNames);
    }

    return propertyNames;
  }

  @Override
  public void setPropertyDelimiter(String delimiter)
      throws PropertyStoreException
  {
    throw new PropertyStoreException(
        "setPropertyDelimiter() not implemented for ZKPropertyStore");
  }

  // put data/child listeners on prefix and all childs
  @Override
  public void subscribeForPropertyChange(String prefix,
      final PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    if (listener == null)
    {
      throw new IllegalArgumentException("listener can't be null. Prefix: " + prefix);
    }

    String path = getAbsolutePath(prefix);

    ZkCallbackHandler<T> callback = null;
    synchronized (_callbackMap)
    {
      Map<PropertyChangeListener<T>, ZkCallbackHandler<T>> callbacks;
      if (!_callbackMap.containsKey(path))
      {
        _callbackMap.put(path, new HashMap<PropertyChangeListener<T>, ZkCallbackHandler<T>>());
      }
      callbacks = _callbackMap.get(path);

      if (!callbacks.containsKey(listener))
      {
        callback = new ZkCallbackHandler<T>(_zkClient, this, prefix, listener);
        callbacks.put(listener, callback);
      }
    }

    try
    {
      if (callback != null)
      {
        // a newly added callback
        _zkClient.subscribeDataChanges(path, callback);
        _zkClient.subscribeChildChanges(path, callback);

        // do initial invocation
        callback.handleChildChange(path, _zkClient.getChildren(path));

        LOG.debug("Subscribed changes for " + path);
      }
    } catch (Exception e)
    {
      LOG.error("Exception in subscribeForPropertyChange(" + prefix + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }
  }

  // prefix is always a normalized key
  private void doUnsubscribeForPropertyChange(String prefix, ZkCallbackHandler<T> callback)
  {
    String path = getAbsolutePath(prefix);

    _zkClient.unsubscribeDataChanges(path, callback);
    _zkClient.unsubscribeChildChanges(path, callback);

    List<String> childs = _zkClient.getChildren(path);
    if (childs == null || childs.size() == 0)
    {
      return;
    }

    for (String child : childs)
    {
      doUnsubscribeForPropertyChange(prefix + "/" + child, callback);
    }
  }

  @Override
  public void unsubscribeForPropertyChange(String prefix,
      PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    if (listener == null)
    {
      throw new IllegalArgumentException("listener can't be null. Prefix: " + prefix);
    }

    String path = getAbsolutePath(prefix);
    ZkCallbackHandler<T> callback = null;

    synchronized (_callbackMap)
    {
      if (_callbackMap.containsKey(path))
      {
        Map<PropertyChangeListener<T>, ZkCallbackHandler<T>> callbacks = _callbackMap.get(path);
        callback = callbacks.remove(listener);

        if (callbacks == null || callbacks.isEmpty())
        {
          _callbackMap.remove(path);
        }
      }
    }

    if (callback != null)
    {
      doUnsubscribeForPropertyChange(prefix, callback);
      LOG.debug("Unsubscribed changes for " + path);
    }
  }

  @Override
  public boolean canParentStoreData()
  {
    return false;
  }

  @Override
  public void setPropertySerializer(final PropertySerializer<T> serializer)
  {
    if (serializer == null)
    {
      throw new IllegalArgumentException("serializer can't be null");
    }

    _serializer = serializer;
  }

  @Override
  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater)
    throws PropertyStoreException
  {
    updatePropertyUntilSucceed(key, updater, true);
  }

  @Override
  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater,
      boolean createIfAbsent) throws PropertyStoreException
  {
    String path = getAbsolutePath(key);
    try
    {
      if (!_zkClient.exists(path))
      {
        if (!createIfAbsent)
        {
          throw new PropertyStoreException("Can't update " + key + " since no node exists");
        } else
        {
          _zkClient.createPersistent(path, true);
        }
      }

      _zkClient.updateDataSerialized(path, new ByteArrayUpdater(updater, _serializer));
    } catch (Exception e)
    {
      LOG.error("Exception in updatePropertyUntilSucceed(" + key + ", " + createIfAbsent + ")", e);
      throw (new PropertyStoreException(e.toString()));
    }

    // update cache
    // getProperty(key);
  }

  @Override
  public boolean compareAndSet(String key, T expected, T update,
      Comparator<T> comparator)
  {
    return compareAndSet(key, expected, update, comparator, true);
  }

  @Override
  public boolean compareAndSet(String key, T expected, T update,
      Comparator<T> comparator, boolean createIfAbsent)
  {
    String path = getAbsolutePath(key);

    // if two threads call with createIfAbsent=true
    // one thread creates the node, the other just goes through
    // when wirteData() one thread writes the other gets ZkBadVersionException
    if (!_zkClient.exists(path))
    {
      if (createIfAbsent)
      {
        _zkClient.createPersistent(path, true);
      } else
      {
        return false;
      }
    }

    try
    {
      Stat stat = new Stat();
      ByteArray currentBytes = _zkClient.readDataAndStat(path, stat, true);
      T current = null;
      if (currentBytes != null)
      {
        current = _serializer.deserialize(currentBytes.getBytes());
      }

      if (comparator.compare(current, expected) == 0)
      {
        byte[] valueBytes = _serializer.serialize(update);
        _zkClient.writeData(path, new ByteArray(valueBytes), stat.getVersion());

        // update cache
        // getProperty(key);

        return true;
      }
    } catch (ZkBadVersionException e)
    {
      LOG.warn("Get BadVersion when writing to zookeeper. Mostly Ignorable due to contention");
    } catch (Exception e)
    {
      LOG.error("Exception when compareAndSet(" + key + ")", e);
    }

    return false;
  }

  @Override
  public boolean exists(String key)
  {
    String path = getAbsolutePath(key);
    return _zkClient.exists(path);
  }

  @Override
  public void handleStateChanged(KeeperState state) throws Exception
  {
    LOG.info("KeeperState:" + state);
    switch (state)
    {
    case SyncConnected:
        _isConnected = true;
        break;
    case Disconnected:
        _isConnected = false;
        break;
    case Expired:
        _isConnected = false;
        _hasSessionExpired = true;
        break;
    }
  }

  @Override
  public void handleNewSession() throws Exception
  {
    ZkConnection connection = ((ZkConnection) _zkClient.getConnection());
    ZooKeeper zookeeper = connection.getZookeeper();
    LOG.info("handleNewSession: " + zookeeper.getSessionId());

    synchronized (_callbackMap)
    {
      for (String path : _callbackMap.keySet())
      {
        Map<PropertyChangeListener<T>, ZkCallbackHandler<T>> callbacks = _callbackMap.get(path);
        if (callbacks == null || callbacks.size() == 0)
        {
          LOG.error("Get a null callback map. Remove it. Path: " + path);
          _callbackMap.remove(path);
          continue;
        }

        for (PropertyChangeListener<T> listener : callbacks.keySet())
        {
          ZkCallbackHandler<T> callback = callbacks.get(listener);

          if (callback == null)
          {
            LOG.error("Get a null callback. Remove it. Path: " + path + ", listener: " + listener);
            callbacks.remove(listener);
            continue;
          }
          _zkClient.subscribeDataChanges(path, callback);
          _zkClient.subscribeChildChanges(path, callback);

          // do initial invocation
          callback.handleChildChange(path, _zkClient.getChildren(path));
        }
      }
    }
  }

  @Override
  public boolean start()
  {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean stop()
  {
    // TODO Auto-generated method stub
    return false;
  }

}

package com.linkedin.clustermanager.store.zk;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertySerializer;
import com.linkedin.clustermanager.store.PropertyStat;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.store.PropertyStoreException;

public class ZKPropertyStore<T> implements PropertyStore<T>, IZkDataListener, IZkChildListener
{

  private final String ROOT = "/";
  private static Logger LOG = Logger.getLogger(ZKPropertyStore.class);

  protected final ZkConnection _zkConnection;
  protected final ZkClient _zkClient;
  protected final PropertySerializer<T> _serializer;
  protected final String _rootPath;

  private Map<String, Map<PropertyChangeListener<T>, ZKPropertyListenerTuple>> _listenerMap 
      = new ConcurrentHashMap<String, Map<PropertyChangeListener<T>, ZKPropertyListenerTuple>>();

  // TODO property cache needs to have a bounded capacity
  private Map<String, PropertyInfo<T>> _propertyCacheMap = new ConcurrentHashMap<String, PropertyInfo<T>>();

  // 1-1 mapping from a PropertyChangeListener<T> to a tuple of { IZkxxx
  // listeners }
  private class ZKPropertyListenerTuple
  {
    public final IZkDataListener _zkDataListener;
    public final IZkChildListener _zkChildListener;

    public ZKPropertyListenerTuple(final PropertyChangeListener<T> listener)
    {
      _zkDataListener = new IZkDataListener()
      {

        @Override
        public void handleDataChange(String dataPath, Object data)
            throws Exception
        {
          if (LOG.isDebugEnabled())
          {
            LOG.debug("data change, " + dataPath + " : " + data);
          }
          listener.onPropertyChange(getRelativePath(dataPath));
        }


        @Override
        public void handleDataDeleted(String dataPath) throws Exception
        {
          if (LOG.isDebugEnabled())
          {
            LOG.debug("data delete, " + dataPath);
          }
          
          /*
          // unsubscribeForPropertyChange(getRelativePath(dataPath), listener);

          // synchronize is necessary, race condition:
          // 1) thread-1 subscribes dataPath and not yet put the listener to map
          // 2) thread-2 deletes dataPath
          // 3) thread-1 put listener to map
          synchronized (_listenerMap)
          {
            _listenerMap.remove(dataPath);
          }
          */
        }

      };

      // TODO implement remove listener
      _zkChildListener = new IZkChildListener()
      {

        @Override
        public void handleChildChange(String parentPath,
            List<String> currentChilds) throws Exception
        {
          LOG.debug("childs change, " + parentPath + " : " + 
                    currentChilds);

          // add child/data listener to nodes
          if (currentChilds != null)
          {
            for (String child : currentChilds)
            {
              String childPath = parentPath + "/" + child;
              _zkClient.subscribeDataChanges(childPath, _zkDataListener);
              _zkClient.subscribeChildChanges(childPath, this);
              
              // do initial invocation
              Object data = _zkClient.readData(childPath);
              _zkDataListener.handleDataChange(childPath, data);
              this.handleChildChange(childPath, _zkClient.getChildren(childPath));
            }
          }
          
          listener.onPropertyChange(getRelativePath(parentPath));
          
        }

      };

    }
  }

  public ZKPropertyStore(ZkConnection zkConnection,
      final PropertySerializer<T> serializer)
  {
    this(zkConnection, serializer, "/");
  }

  public ZKPropertyStore(ZkConnection zkConnection,
      final PropertySerializer<T> serializer, String rootPath)
  {
    _serializer = serializer;
    _zkConnection = zkConnection;
    _zkClient = new ZkClient(_zkConnection);
    setPropertySerializer(serializer);

    // Strip off leading slash
    while (rootPath.startsWith("/"))
    {
      rootPath = rootPath.substring(1, rootPath.length());
    }

    _rootPath = "/" + rootPath;

  }

  private String getPath(String key)
  {
    // Strip off leading slash
    while (key.startsWith("/"))
    {
      key = key.substring(1, key.length());
    }

    // String path = key.equals(ROOT) ? _rootPath : (_rootPath + "/" + key);
    String path = key.equals("") ? _rootPath : (_rootPath + "/" + key);

    return path;
  }

  private String getRelativePath(String path)
  {
    // strip off rootPath from path
    if (!path.startsWith(_rootPath))
    {
      LOG.warn("path does NOT start with: " + _rootPath);
      return path;
    }

    if (path.equals(_rootPath))
      return ROOT;

    path = path.substring(_rootPath.length());

    return path;
  }

  private void updatePropertyCache(String path) 
  // throws PropertyStoreException
  {
    try
    {
      synchronized (_propertyCacheMap)
      {

        Stat stat = new Stat();
        T value = _zkClient.<T> readData(path, stat);

        // cache it
        _propertyCacheMap.put(path,
            new PropertyInfo<T>(value, stat, stat.getVersion()));
      }
    } catch (ZkNoNodeException e)
    {
      // This is OK
    }
  }

  @Override
  public void createPropertyNamespace(String prefix)
  {
    String path = getPath(prefix);
    if (!_zkClient.exists(path))
    {
      _zkClient.createPersistent(path, true);
    }
  }

  @Override
  public void setProperty(String key, final T value)
      throws PropertyStoreException
  {
    String path = getPath(key);
    _zkClient.createPersistent(path, true);

    // it depends on the serializer to handle value == null
    _zkClient.writeData(path, value);
   
    // update cache immediately
    updatePropertyCache(path);
  }

  @Override
  public T getProperty(String key) throws PropertyStoreException
  {
    return getProperty(key, null);
  }

  @Override
  public T getProperty(String key, PropertyStat propertyStat)
      throws PropertyStoreException
  {
    String path = getPath(key);

    T value = null;

    try
    {
      if (_propertyCacheMap.containsKey(path))
      {
        PropertyInfo<T> propertyInfo = _propertyCacheMap.get(path);

        value = propertyInfo._value;

        if (propertyStat != null)
        {
          propertyStat.setLastModifiedTime(propertyInfo._stat.getMtime());
          propertyStat.setVersion(propertyInfo._version);
        }
      } else
      {
        value = readData(path, propertyStat);
      }
    } catch (Exception e)
    {
      // System.err.println(e.getMessage());
      LOG.warn(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }

    // return a copy
    // TODO optimize to save serialize/de-serialize by caching only byte[]
    if (value != null)
    {
      value = _serializer.deserialize(_serializer.serialize(value));
    }
    return value;
  }

  // read data without going to cache
  private T readData(String path, PropertyStat propertyStat)
      throws PropertyStoreException
  {
    try
    {
      synchronized (_propertyCacheMap)
      {
        if (!_propertyCacheMap.containsKey(path))
        {
          Stat stat = new Stat();
          T value = _zkClient.<T> readData(path, stat);

          if (propertyStat != null)
          {
            propertyStat.setLastModifiedTime(stat.getMtime());
            propertyStat.setVersion(stat.getVersion());
          }

          // cache it
          _propertyCacheMap.put(path,
              new PropertyInfo<T>(value, stat, stat.getVersion()));
          _zkClient.subscribeDataChanges(path, this);
        }
        return _propertyCacheMap.get(path)._value;
      }
    } catch (ZkNoNodeException e)
    {
      return null;
    } catch (Exception e)
    {
      // System.err.println(e.getMessage());
      // _logger.warn(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }
  }

  @Override
  public void removeProperty(String key) throws PropertyStoreException
  {
    String path = getPath(key);

    try
    {
      _zkClient.delete(path);
    } catch (Exception e)
    {
      // System.err.println(e.getMessage());
      LOG.warn(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }
    
    // update local cache immediately
    synchronized(_propertyCacheMap)
    {
      _propertyCacheMap.remove(path);
    }
  }

  @Override
  public String getPropertyRootNamespace()
  {
    return _rootPath;
  }

  public void removeRootNamespace() throws PropertyStoreException
  {
    removeNamespace(ROOT);
  }

  @Override
  public void removeNamespace(String prefix) throws PropertyStoreException
  {
    String path = getPath(prefix);

    _zkClient.deleteRecursive(path);
    // removePropertyRecursive() triggers listeners which refresh cache
  }

  private void doGetPropertyNames(String path, List<String> leafNodes) 
  throws PropertyStoreException
  {
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
      getProperty(getRelativePath(path));
      leafNodes.add(getRelativePath(path));
      return;
    }
    
    for (String child : childs)
    {
      String pathToChild = path + "/" + child;
      doGetPropertyNames(pathToChild, leafNodes);
    }
  }

  @Override
  public List<String> getPropertyNames(String prefix)
      throws PropertyStoreException
  {
    String path = getPath(prefix);
    List<String> propertyNames = new ArrayList<String>();
    doGetPropertyNames(path, propertyNames);
    
    /*
    if (!_zkClient.exists(path))
      return null;

    List<String> children = _zkClient.getChildren(path);

    List<String> propertyNames = new ArrayList<String>();
    for (String child : children)
    {
      String pathToChild = path + "/" + child;
      propertyNames.add(getRelativePath(pathToChild));

      // cache all child property values
      getProperty(getRelativePath(pathToChild));

    }
    */
    
    return propertyNames;
  }

  @Override
  public void setPropertyDelimiter(String delimiter)
      throws PropertyStoreException
  {
    throw new PropertyStoreException(
        "setPropertyDelimiter() not implemented for ZKPropertyStore");
  }

  public void subscribeForRootPropertyChange(
      final PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    subscribeForPropertyChange(ROOT, listener);
  }

  // put listener on prefix and all its children
  @Override
  public void subscribeForPropertyChange(String prefix,
      final PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    String path = getPath(prefix);

    // Map<PropertyChangeListener<T>, ZKPropertyListenerTuple>
    // listenerMapForPath = null;
    synchronized (_listenerMap)
    {
      if (!_zkClient.exists(path))
        return;

      Map<PropertyChangeListener<T>, ZKPropertyListenerTuple> listenerMapForPath = _listenerMap
          .get(path);
      if (listenerMapForPath == null)
      {
        listenerMapForPath = new ConcurrentHashMap<PropertyChangeListener<T>, ZKPropertyListenerTuple>();
        _listenerMap.put(path, listenerMapForPath);
      }

      if (listenerMapForPath.get(listener) == null)
      {
        ZKPropertyListenerTuple listenerTuple = new ZKPropertyListenerTuple(
            listener);
        listenerMapForPath.put(listener, listenerTuple);

        _zkClient.subscribeDataChanges(path, listenerTuple._zkDataListener);
        _zkClient.subscribeChildChanges(path, listenerTuple._zkChildListener);
        
        // do initial invocation
        try
        {
          T data = _zkClient.<T>readData(path);
          listenerTuple._zkDataListener.handleDataChange(path, data);
          listenerTuple._zkChildListener.handleChildChange(path, _zkClient.getChildren(path));
        }
        catch(Exception e)
        {
          e.printStackTrace();
        }

      }

    }
  }

  public void unsubscribeForRootPropertyChange(
      PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    unsubscribeForPropertyChange(ROOT, listener);
  }

  @Override
  public void unsubscribeForPropertyChange(String prefix,
      PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    throw new PropertyStoreException("unsubscribe not implememted");
    
    /*
    String path = getPath(prefix);

    synchronized (_listenerMap)
    {
      final Map<PropertyChangeListener<T>, ZKPropertyListenerTuple> listenerMapForPath = _listenerMap
          .get(path);
      if (listenerMapForPath != null)
      {
        ZKPropertyListenerTuple listenerTuple = listenerMapForPath
            .remove(listener);

        if (listenerTuple != null)
        {

          List<String> nodes = BFS(path, MAX_DEPTH, null, null);
          for (String node : nodes)
          {
            _zkClient.unsubscribeChildChanges(node,
                listenerTuple._zkChildListener);
            _zkClient.unsubscribeDataChanges(node,
                listenerTuple._zkDataListener);
          }

        }
      }

      if (listenerMapForPath == null || listenerMapForPath.isEmpty())
      {
        _listenerMap.remove(path);
      }

    }
    */
  }

  @Override
  public boolean canParentStoreData()
  {

    return false;
  }

  @Override
  public void setPropertySerializer(final PropertySerializer<T> serializer)
  {

    ZkSerializer zkSerializer = new ZkSerializer()
    {

      @SuppressWarnings("unchecked")
      @Override
      public byte[] serialize(Object data) throws ZkMarshallingError
      {

        try
        {
          byte[] bytes = serializer.serialize((T) data);
          return bytes;
        } catch (PropertyStoreException e)
        {

          e.printStackTrace();
          throw new ZkMarshallingError(e.getMessage());
        }
      }

      @Override
      public Object deserialize(byte[] bytes) throws ZkMarshallingError
      {

        try
        {
          Object obj = serializer.deserialize(bytes);
          return obj;
        } catch (PropertyStoreException e)
        {

          e.printStackTrace();
          throw new ZkMarshallingError(e.getMessage());
        }
      }

    };

    _zkClient.setZkSerializer(zkSerializer);
  }

  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater,
      boolean createIfAbsent)
  {
    String path = getPath(key);
    if (!_zkClient.exists(path))
    {
      if (!createIfAbsent)
      {
        return;
      } else
      {
        _zkClient.createPersistent(path, true);
      }
    }
  
    _zkClient.<T>updateDataSerialized(path, updater);
    
    // update cache immediately
    updatePropertyCache(path);
  }

  @Override
  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater)
  {
    updatePropertyUntilSucceed(key, updater, true);
  }

  /*
  @Override 
  public boolean updateProperty(String key, DataUpdater<T> updater)
  { 
    String path = getPath(key); 
    if (!_zkClient.exists(path)) 
       return false;
    Stat stat = new Stat(); 
    boolean isSucceed = false;
    
    try 
    { 
      T oldData = _zkClient.<T>readData(path, stat); 
      T newData = updater.update(oldData); 
      _zkClient.writeData(path, newData,
      stat.getVersion()); // callback will update cache isSucceed = true; 
    } catch (ZkBadVersionException e) 
    { 
      isSucceed = false; 
    }
    return isSucceed; 
  }
  */

  @Override
  public boolean compareAndSet(String key, T expected, T update,
      Comparator<T> comparator)
  {
    return compareAndSet(key, expected, update, comparator, false);
  }

  @Override
  public boolean compareAndSet(String key, T expected, T update,
      Comparator<T> comparator, boolean createIfAbsent)
  {
    String path = getPath(key);

    // assume two threads call with createIfAbsent=true
    // one thread will create the node, and the other just goes through
    // when wirteData() gets invoked, one thread will get the right version to write
    // while the other thread will not and thus gets ZkBadVersionException
    if (createIfAbsent)
    {
      _zkClient.createPersistent(path, true);
    }

    if (!_zkClient.exists(path))
      return false;

    Stat stat = new Stat();
    boolean isSucceed = false;

    try
    {
      T current = _zkClient.<T> readData(path, stat);

      if (comparator.compare(current, expected) == 0)
      {
        _zkClient.writeData(path, update, stat.getVersion());
        
        // update local cache immediately
        updatePropertyCache(path);
        
        isSucceed = true;
      }
    } catch (ZkBadVersionException e)
    {
      isSucceed = false;
    }

    return isSucceed;
  }

  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception
  {
    LOG.info("update-cache: " + dataPath + ": data changed to "
        + data);
    updatePropertyCache(dataPath);

  }

  // TODO: unreliable
  @Override
  public void handleDataDeleted(String dataPath) throws Exception
  {
    LOG.info("update-cache: " + dataPath + ": data deleted");
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds) 
  throws Exception
  {
    synchronized (_propertyCacheMap)
    {
      
      if (currentChilds == null)
      {
          // race condition:
          // 1) thread-1 reads from ZK and not yet put the value to map
          // 2) thread-2 deletes it from ZK and remove it from map
          // 3) thread-1 put the value to map
       
          _propertyCacheMap.remove(parentPath);
      }
      else
      {
        // iterate cache map 
        // remove all values with keys starting with parentPath
        // add all currentChilds to cache map
        Iterator iter = _propertyCacheMap.entrySet().iterator();
        while (iter.hasNext()) 
        {
          Map.Entry<String, PropertyInfo<T>> entry = (Map.Entry)iter.next();
          String key = entry.getKey();
          if (key.startsWith(parentPath))
          {
            iter.remove();
          }
          
        }
        
        for(String child : currentChilds)
        {
          String childPath = parentPath + "/" + child;
          updatePropertyCache(childPath);
        }
  
      }
    }
    
  }

  @Override
  public boolean exists(String key)
  {
    String path = getPath(key);
    return _zkClient.exists(path);
  }

}

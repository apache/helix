package com.linkedin.clustermanager.store.zk;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.store.PropertyChangeListener;
import com.linkedin.clustermanager.store.PropertySerializer;
import com.linkedin.clustermanager.store.PropertyStat;
import com.linkedin.clustermanager.store.PropertyStore;
import com.linkedin.clustermanager.store.PropertyStoreException;

public class ZKPropertyStore<T> implements PropertyStore<T>
{
  private final String _ROOT = "";
  private final int _MAX_DEPTH = 3; // max depth for adding listeners
  private static Logger _logger = Logger.getLogger(ZKPropertyStore.class);
  
  protected final ZkClient _zkClient;
  protected final PropertySerializer _serializer;
  protected final String _rootPath;

  private Map<String, Map< PropertyChangeListener<T>, ZKPropertyListenerTuple> > 
        _listenerMap = new ConcurrentHashMap<String, Map<PropertyChangeListener<T>, ZKPropertyListenerTuple>>();
  
  private Map<String, PropertyInfo> _cacheMap = new ConcurrentHashMap< String, PropertyInfo>();

  // statistics numbers
  private long _nrReads = 0;
  private long _nrHits = 0;

  private class PathnDepth
  {
    public String _path;
    public int _depth;

    public PathnDepth(String path, int depth)
    {
      _path = path;
      _depth = depth;
    }
  }
  
  // 1-1 mapping from a PropertyChangeListener<T> to a tuple of { IZkxxx listeners }
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
          System.out.println(dataPath + ": data changed to " + data);
          
          // need to change local {data, stat} cache to keep consistency
          readData(dataPath, null);
          
          listener.onPropertyChange(getRelativePath(dataPath));
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception
        {
          System.out.println("property deleted at " + dataPath);

          unsubscribeForPropertyChange(getRelativePath(dataPath), listener);
          
          // need to remove from local {data, stat} cache
          _cacheMap.remove(dataPath);
          _listenerMap.remove(dataPath);
        }

      };

      _zkChildListener = new IZkChildListener()
      {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
        {
          System.out.println("children changed at " + parentPath + ": " + currentChilds);
          
          
          // List<String> nonleaf = new ArrayList<String>();
          List<String> leaf = new ArrayList<String>();
          
          // TODO: should not allow to go 
          //  beyond _MAX_DEPTH levels down from the original property listener
          List<String> nodes = BFS(parentPath, _MAX_DEPTH, null, leaf);
          
          // add child/data listener to nodes
          for (String node : nodes)
          {
            _zkClient.subscribeChildChanges(node, this);
            _zkClient.subscribeDataChanges(node, _zkDataListener);  
          }
          
          // refresh cache
          for (String node : leaf)
          {
            readData(node, null);
            listener.onPropertyChange(getRelativePath(node));
          }
          
        }
        
      };
      
    }
  }

  public ZKPropertyStore(ZkClient zkClient, final PropertySerializer serializer)
  {
    this(zkClient, serializer, "/");
  }
  
  public ZKPropertyStore(ZkClient zkClient, final PropertySerializer serializer, String rootPath)
  {
    _serializer = serializer;

    _zkClient = zkClient;
    setPropertySerializer(serializer);
    
    // Strip off leading slash
    while (rootPath.startsWith("/"))
    {
      rootPath = rootPath.substring(1, rootPath.length());
    }
    
    _rootPath = "/" + rootPath;
  }

  public double getHitRatio()
  {
    // System.out.println("Reads(" + _nrReads + ")/Hits(" + _nrHits + ")");
    _logger.info("Reads(" + _nrReads + ") / Hits(" + _nrHits + ")");
    
    if (_nrReads == 0)
      return 0.0;
    
    return (double) _nrHits / _nrReads;
  }

  private String getPath(String key)
  {
    // Strip off leading slash
    while (key.startsWith("/"))
    {
      key = key.substring(1, key.length());
    }

    String path = key.equals(_ROOT) ? _rootPath : (_rootPath + "/" + key);

    return path;
  }

  private String getRelativePath(String path)
  {
    // strip off rootPath from path
    if (!path.startsWith(_rootPath))
    {
      _logger.warn("path does NOT start with: " + _rootPath);
      return path;
    }

    if (path.equals(_rootPath))
      return _ROOT;
    
    path = path.substring(_rootPath.length() + 1);

    return path;
  }

  @Override
  public void createPropertyNamespace(String prefix)
  {
    String path = getPath(prefix);

    // create recursively all non-exist nodes
    String parent = path;
    LinkedList<String> pathsToCreate = new LinkedList<String>();
    while (parent.length() > 0 && !_zkClient.exists(parent))
    {
      pathsToCreate.push(parent);
      parent = parent.substring(0, parent.lastIndexOf('/'));
    }

    while (pathsToCreate.size() > 0)
    {
      String node = pathsToCreate.pop();
      _zkClient.createPersistent(node);
    }
    
  }
  
  @Override
  public synchronized void setProperty(String key, final T value)
      throws PropertyStoreException
  {
    String path = getPath(key);
    createPropertyNamespace(key);
    
    if (value != null)
      _zkClient.writeData(path, value);
    
    // setProperty() triggers either child/data listener
    // which in turn update cache
  }

  // BFS with a given depth
  // nonleaf nodes' paths go to nonleaf
  // leaf nodes' paths go to leaf
  // return list of all nodes (including root node)
  private List<String> BFS(String prefix, int depth, List<String> nonleaf, List<String> leaf)
  {
    List<String> nodes = new ArrayList<String>();
    
    if (nonleaf != null)
      nonleaf.clear();

    if (leaf != null)
      leaf.clear();

    if (!_zkClient.exists(prefix))
      return nodes;
    
    LinkedList<PathnDepth> queue = new LinkedList<PathnDepth>();
    queue.push(new PathnDepth(prefix, 0));
    while (!queue.isEmpty())
    {
      PathnDepth node = queue.pop();
      List<String> children = _zkClient.getChildren(node._path);
      if (children == null || children.isEmpty())
      {
        nodes.add(node._path);
        if (leaf != null)
          leaf.add(node._path);
        
        continue;
      }
      
      nodes.add(node._path);
      
      if (nonleaf != null)
        nonleaf.add(node._path);
      
      if (node._depth >= depth)
        continue;

      for (String child : children)
      {
      	String pathToChild = node._path + "/" + child;
      	queue.push(new PathnDepth(pathToChild, node._depth+1));
      }
    }
    
    return nodes;
  }

  @Override
  public T getProperty(String key) throws PropertyStoreException
  {
    return getProperty(key, null);
  }
  

  @SuppressWarnings("unchecked")
  @Override
  public synchronized T getProperty(String key, PropertyStat propertyStat) throws PropertyStoreException
  {
    String path = getPath(key);
    _nrReads++;

    T value = null;

    try
    {
      PropertyInfo propertyInfo = _cacheMap.get(path);

      if (propertyInfo != null)
      {
        _nrHits++;
        value = (T) propertyInfo._value;
        
        if (propertyStat != null)
        {
          propertyStat.setLastModifiedTime(propertyInfo._stat.getMtime());
          propertyStat.setVersion(propertyInfo._version);
        }
      } 
      else
      {
          value = readData(path, propertyStat);
      }
    } 
    catch (Exception e) // ZkNoNodeException
    {
      // System.err.println(e.getMessage());
      _logger.warn(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }
    
    return value;
  }
  
  // read data without going to cache
  private synchronized T readData(String path, PropertyStat propertyStat) throws PropertyStoreException
  {
    T value = null;
    Stat stat = new Stat();
    
    try
    {
      value = _zkClient.<T>readData(path, stat);
      
      if (propertyStat != null)
      {
        propertyStat.setLastModifiedTime(stat.getMtime());
        propertyStat.setVersion(stat.getVersion());
      }
      
      // cache it
      _cacheMap.put(path, new PropertyInfo(value, stat, stat.getVersion()));
    } 
    catch (Exception e) // ZkNoNodeException
    {
      // System.err.println(e.getMessage());
      // _logger.warn(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }
    
    return value;
  }

  
  @Override
  public synchronized void removeProperty(String key) throws PropertyStoreException
  {
    String path = getPath(key);

    try
    {
      _zkClient.delete(path);
    } 
    catch (Exception e)
    {
      // System.err.println(e.getMessage());
      _logger.warn(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }
    // removerProperty() triggers listener which update cache
  }

  @Override
  public String getPropertyRootNamespace()
  {
    return _rootPath;
  }

  public void removeRootProperty() throws PropertyStoreException
  {
    removeProperyRecursive(_ROOT);
  }

  public synchronized void removeProperyRecursive(String key) throws PropertyStoreException
  {
    String path = getPath(key);

    _zkClient.deleteRecursive(path);
    // removePropertyRecursive() triggers listeners which refresh cache
  }

  @Override
  public List<String> getPropertyNames(String prefix) throws PropertyStoreException
  {
    String path = getPath(prefix);

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

    return propertyNames;
  }

  @Override
  public void setPropertyDelimiter(String delimiter) throws PropertyStoreException
  {
    throw new PropertyStoreException("setPropertyDelimiter() not implemented for ZKPropertyStore");
  }

  public void subscribeForRootPropertyChange(final PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    subscribeForPropertyChange(_ROOT, listener);
  }

  // put listener on prefix and all its children
  @Override
  public void subscribeForPropertyChange(String prefix,
      final PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    String path = getPath(prefix);
    if (!_zkClient.exists(path))
      return;

    Map<PropertyChangeListener<T>, ZKPropertyListenerTuple> listenerMapForPath = null;
    synchronized (_listenerMap)
    {
      listenerMapForPath = _listenerMap.get(path);
      if (listenerMapForPath == null)
      {
        listenerMapForPath = new ConcurrentHashMap<PropertyChangeListener<T>, ZKPropertyListenerTuple>();
        _listenerMap.put(path, listenerMapForPath);
      }

      if (listenerMapForPath.get(listener) == null)
      {
        ZKPropertyListenerTuple listenerTuple = new ZKPropertyListenerTuple(listener);
        listenerMapForPath.put(listener, listenerTuple);

      	List<String> nodes = BFS(path, _MAX_DEPTH, null, null);
      
      	for (String node: nodes)
      	{
          _zkClient.subscribeChildChanges(node, listenerTuple._zkChildListener);
          _zkClient.subscribeDataChanges(node, listenerTuple._zkDataListener);
      	}
      	
      }

    }
  }

  public void unsubscribeForRootPropertyChange(PropertyChangeListener<T> listener) throws PropertyStoreException
  {
    unsubscribeForPropertyChange(_ROOT, listener);
  }

  @Override
  public void unsubscribeForPropertyChange(String prefix, PropertyChangeListener<T> listener) 
    throws PropertyStoreException
  {

    String path = getPath(prefix);
    // if (!_zkClient.exists(path))
    // return;

    synchronized (_listenerMap)
    {
      final Map<PropertyChangeListener<T>, ZKPropertyListenerTuple> listenerMapForPath = _listenerMap.get(path);
      if (listenerMapForPath != null)
      {
        ZKPropertyListenerTuple listenerTuple = listenerMapForPath.remove(listener);

        if (listenerTuple != null)
        {

          List<String> nodes = BFS(path, _MAX_DEPTH, null, null);
          for (String node: nodes)
          {
            _zkClient.unsubscribeChildChanges(node, listenerTuple._zkChildListener);
            _zkClient.unsubscribeDataChanges(node, listenerTuple._zkDataListener);
          }
        
        }
      }

      if (listenerMapForPath == null || listenerMapForPath.isEmpty())
      {
        _listenerMap.remove(path);
      }

    }
  }

  @Override
  public boolean canParentStoreData()
  {
    
    return false;
  }

  @Override
  public void setPropertySerializer(final PropertySerializer serializer)
  {
    
    ZkSerializer zkSerializer = new ZkSerializer()
    {

      @Override
      public byte[] serialize(Object data) throws ZkMarshallingError
      {
        
        byte[] bytes = null;
        try
        {
          bytes = serializer.serialize(data);
        } 
        catch (PropertyStoreException e)
        {
          
          e.printStackTrace();
          throw new ZkMarshallingError(e.getMessage());
        }
        return bytes;
      }

      @Override
      public Object deserialize(byte[] bytes) throws ZkMarshallingError
      {
        
        Object obj = null;
        try
        {
          obj = serializer.deserialize(bytes);
        } 
        catch (PropertyStoreException e)
        {
          
          e.printStackTrace();
          throw new ZkMarshallingError(e.getMessage());
        }

        return obj;
      }

    };

    _zkClient.setZkSerializer(zkSerializer);
  }

  @Override
  public void updatePropertyUntilSucceed(String key, DataUpdater<T> updater)
  {
    String path = getPath(key);
    if (!_zkClient.exists(path))
      return;
    
    _zkClient.<T>updateDataSerialized(path, updater);
    // callback will update cache
  }
  
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
      _zkClient.writeData(path, newData, stat.getVersion());
      // callback will update cache
      isSucceed = true;
    } 
    catch (ZkBadVersionException e) 
    {
      isSucceed = false;
    }
    
    return isSucceed;
  }

  @Override
  public boolean compareAndSet(String key, T expected, T update, Comparator<T> comparator)
  {
    String path = getPath(key);
    if (!_zkClient.exists(path))
      return false;
    
    Stat stat = new Stat();
    boolean isSucceed = false;
    
    try 
    {
      T current = _zkClient.<T>readData(path, stat);
      
      if (comparator.compare(current, expected) == 0)
      {
        _zkClient.writeData(path, update, stat.getVersion());
        isSucceed = true;
      }
    } 
    catch (ZkBadVersionException e) 
    {
      isSucceed = false;
    }
    
    return isSucceed;
  }
}

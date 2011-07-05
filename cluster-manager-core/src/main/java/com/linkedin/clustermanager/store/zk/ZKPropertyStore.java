package com.linkedin.clustermanager.store.zk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
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

  private Map<String, Map<PropertyChangeListener<T>, ZKPropertyListenerTuple>> _listenerMap = new ConcurrentHashMap<String, Map<PropertyChangeListener<T>, ZKPropertyListenerTuple>>();
  private Map< String, List<PropertyInfo> > _cacheMap = new ConcurrentHashMap< String, List<PropertyInfo> >();

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
          readDataByVersion(dataPath, null);
          
          String pathNoVersion = dataPath.substring(0, dataPath.lastIndexOf('/')); // strip off version number at end
          listener.onPropertyChange(getRelativePath(pathNoVersion));
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception
        {
          System.out.println("property deleted at " + dataPath);

          unsubscribeForPropertyChange(getRelativePath(dataPath), listener);
          
          // need to remove from local {data, stat} cache
          int version = Integer.parseInt(dataPath.substring(dataPath.lastIndexOf('/')+1, dataPath.length()));
       
          String pathNoVersion = dataPath.substring(0, dataPath.lastIndexOf('/'));  // strip off version number at end
          List<PropertyInfo> propertyInfoList = _cacheMap.get(pathNoVersion);
          if (propertyInfoList != null)
          {
            PropertyInfo.removePropertyInfoFromList(propertyInfoList, version);
            if (propertyInfoList.size() == 0)
              _cacheMap.remove(pathNoVersion);
          }
            
          _listenerMap.remove(pathNoVersion);
        }

      };

      _zkChildListener = new IZkChildListener()
      {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
        {
          System.out.println("children changed at " + parentPath + ": " + currentChilds);
          
          List<String> nonleaf = new ArrayList<String>();
          List<String> leaf = new ArrayList<String>();
          List<String> validKeys = new ArrayList<String>(); // keys that has leaf
          
          // may go beyond _MAX_DEPTH levels down from the original property listener
          BFS(parentPath, _MAX_DEPTH, nonleaf, leaf);
          
          // add child listener to non-leaf nodes
          for (String node : nonleaf)
          {
            _zkClient.subscribeChildChanges(node, this);
            if (isValidKey(node))
            {
              validKeys.add(node);
            }
          }
        
          // add data listener to leaf node 
          // ensure leaf node has a valid version number in path at end
          for (String node : leaf)
          {
            if (!isValidLeaf(node))
              _zkClient.subscribeChildChanges(node, this);
            else
              _zkClient.subscribeDataChanges(node, _zkDataListener);
          }
          
          // refresh cache
          for (String key : validKeys)
          {
            readData(key, null);
            listener.onPropertyChange(getRelativePath(key));
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
    
    // Strip off leading / trailing slash
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
    // Strip off leading / trailing slash
    while (key.startsWith("/"))
    {
      key = key.substring(1, key.length());
    }

    String path = key.equals(_ROOT)? _rootPath : (_rootPath + "/" + key);

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
  public synchronized void setProperty(String key, final T value)
      throws PropertyStoreException
  {
    String path = getPath(key);

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

    // if value == null, just create a persistent node
    if (value != null)
      _zkClient.writeData(path, value);
    
    // setProperty() triggers either child/data listener
    // which in turn update cache
  }

  // a valid leaf has version number at end
  private boolean isValidLeaf(String path)
  {
    String version = path.substring(path.lastIndexOf('/')+1, path.length());
    try 
    {
      Integer.parseInt(version);
    }
    catch (NumberFormatException e)
    {
      return false;
    }
    return true;
  }
  
  // a valid key has valid leaf as its children
  private boolean isValidKey(String path)
  {
    List<String> children = _zkClient.getChildren(path);
        
    for (String child : children)
    {
      String pathToChild = path + "/" + child;
      if (isValidLeaf(pathToChild))
        return true;
    }
    
    return false;
  }
  
  // BFS with a given depth
  // nonleaf nodes' paths go to nonleaf
  // leaf nodes' paths go to leaf
  private void BFS(String prefix, int depth, List<String> nonleaf, List<String> leaf)
  {
    if (nonleaf == null)
      nonleaf = new ArrayList<String>();

    if (leaf == null)
      leaf = new ArrayList<String>();

    LinkedList<PathnDepth> queue = new LinkedList<PathnDepth>();
    queue.push(new PathnDepth(prefix, 0));
    while (!queue.isEmpty())
    {
      PathnDepth node = queue.pop();
      List<String> children = _zkClient.getChildren(node._path);
      if (children == null || children.isEmpty())
      {
        leaf.add(node._path);
        continue;
      }
      nonleaf.add(node._path);
      
      if (node._depth >= depth)
      {
        continue;
      }

      for (String child : children)
      {
      	String pathToChild = node._path + "/" + child;
      	queue.push(new PathnDepth(pathToChild, node._depth+1));
      }
    }

  }

  @Override
  public List<T> getProperty(String key) throws PropertyStoreException
  {
    return getProperty(key, null);
  }
  
  public T getPropertyByVersion(String key) throws PropertyStoreException
  {
    return getPropertyByVersion(key, null);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public synchronized List<T> getProperty(String key, List<PropertyStat> propertyStatList) throws PropertyStoreException
  {
    String path = getPath(key);
    _nrReads++;

    List<T> valueList = new ArrayList<T>();

    List<PropertyInfo> propertyInfoList = _cacheMap.get(path);

    if (propertyInfoList != null && propertyInfoList.size() > 0)
    {
      _nrHits++;
      /**
      PropertyInfo propertyInfo = propertyInfoList.get(0); // the latest version is at head
      value = (T) propertyInfo._value;
      
      if (propertyStat != null)
      {
        propertyStat.setLastModifiedTime(propertyInfo._stat.getMtime());
        propertyStat.setVersion(propertyInfo._version);
      }
      **/
      for (PropertyInfo info : propertyInfoList)
      {
        valueList.add( (T) info._value);
        
        if (propertyStatList != null)
          propertyStatList.add(new PropertyStat(info._stat.getMtime(), info._version));
      }
      
    } 
    else
    {
      try
      {
        valueList = readData(path, propertyStatList);
      } 
      catch (Exception e) // ZkNoNodeException
      {
        // System.err.println(e.getMessage());
        _logger.warn(e.getMessage());
        throw (new PropertyStoreException(e.getMessage()));
      }

    }

    return valueList;
    // return value;
  }
  
  @SuppressWarnings("unchecked")
  // @Override
  public synchronized T getPropertyByVersion(String key, PropertyStat propertyStat) throws PropertyStoreException
  {
    String path = getPath(key);
    _nrReads++;

    T value = null;

    try
    {
      int version = Integer.parseInt(path.substring(path.lastIndexOf('/')+1, path.length()));
      String pathNoVersion = path.substring(0, path.lastIndexOf('/'));  // strip off version number at end
      List<PropertyInfo> propertyInfoList = _cacheMap.get(pathNoVersion);

      PropertyInfo propertyInfo = PropertyInfo.findPropertyInfoInList(propertyInfoList, version);
      
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
          value = readDataByVersion(path, propertyStat);
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
  
  // read data without going to cache, 
  // useful when the cached data becomes stale, because some other node changes it on ZK
  // cache and return all versions
  @SuppressWarnings("unchecked")
  private synchronized List<T> readData(String path, List<PropertyStat> propertyStatList) throws PropertyStoreException
  {
    T value = null;
    Stat stat = new Stat();
    
    try
    {
      List<T> valueList = new ArrayList<T>();
      
      // read all versions
      ArrayList<PropertyInfo> propertyInfoList = new ArrayList<PropertyInfo>();
      
      List<String> children = _zkClient.getChildren(path);
      for (String child : children)
      {
        String pathToChild = path + "/" + child;
        value = (T) _zkClient.readData(pathToChild, stat);
        
        int version = Integer.parseInt(child.substring(child.lastIndexOf('/')+1, child.length()));
        propertyInfoList.add(new PropertyInfo(value, stat, version));
      }
      
      if (propertyInfoList.size() > 0)
      {
        // sort it
        PropertyInfo[] propertyInfoArray = new PropertyInfo[propertyInfoList.size()];
        propertyInfoList.toArray(propertyInfoArray);
        
        Arrays.sort(propertyInfoArray, new Comparator<PropertyInfo>() {

          @Override
          public int compare(PropertyInfo property1, PropertyInfo property2)
          {
            return (property2._version - property1._version);
          }

        });
        
        ArrayList<PropertyInfo> sortedPropertyInfoList 
              = new ArrayList<PropertyInfo>(Arrays.asList(propertyInfoArray));
        
        // return sorted list of value and stat
        for (PropertyInfo info : sortedPropertyInfoList)
        {
          valueList.add( (T) info._value);
          
          if (propertyStatList != null)
            propertyStatList.add(new PropertyStat(info._stat.getMtime(), info._version));
        }
        
        /**
        value = (T) sortedPropertyInfoList.get(0)._value;
        if (propertyStat != null)
        {
          Stat latestStat = sortedPropertyInfoList.get(0)._stat;
          propertyStat.setLastModifiedTime(latestStat.getMtime());
          propertyStat.setVersion(sortedPropertyInfoList.get(0)._version);
        }
        **/
        
        _cacheMap.put(path, sortedPropertyInfoList);
      } 
     
      return valueList;
    } 
    catch (Exception e) // ZkNoNodeException
    {
      // System.err.println(e.getMessage());
      // _logger.warn(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }
    
    // return value;
  }
  
  private synchronized T readDataByVersion(String path, PropertyStat propertyStat) throws PropertyStoreException
  {
    T value = null;
    Stat stat = new Stat();
    
    try
    {
      value = _zkClient.<T>readData(path, stat);
      
      int version = Integer.parseInt(path.substring(path.lastIndexOf('/')+1, path.length()));
      // PropertyInfo propertyInfo = new PropertyInfo(value, stat, version);
      
      if (propertyStat != null)
      {
        propertyStat.setLastModifiedTime(stat.getMtime());
        propertyStat.setVersion(version);
      }
      
      String pathNoVersion = path.substring(0, path.lastIndexOf('/'));
      /**
      List<PropertyInfo> propertyInfoList = _cacheMap.get(pathNoVersion);
      if (propertyInfoList == null)
      {
        propertyInfoList = new ArrayList<PropertyInfo>();
        propertyInfoList.add(propertyInfo);
        _cacheMap.put(pathNoVersion, propertyInfoList);
      }
      else
      {
        PropertyInfo.updatePropertyInfoInList(propertyInfoList, propertyInfo);  // need to insert and sort
      }
      **/
      // cache all versions
      readData(pathNoVersion, null);
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
      // remove all versions under the path
      List<String> children = _zkClient.getChildren(path);
      for (String child : children)
      {
        String pathToChild = path + "/" + child;
        _zkClient.delete(pathToChild);
      }

      _zkClient.delete(path);

    } 
    catch (Exception e)
    {
      // System.err.println(e.getMessage());
      _logger.warn(e.getMessage());
      throw (new PropertyStoreException(e.getMessage()));
    }
    // removeProperty() triggers either child/data listener
    // which in turn update cache

  }
  
  public synchronized void removePropertyByVersion(String key) throws PropertyStoreException
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
    // removeropertyByVersion() triggers either child/data listener
    // which in turn update cache
  }

  @Override
  public String getRootPropertyName()
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
    // removePropertyRecursive() triggers listeners
    // which in turn refresh cache
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
      getProperty(pathToChild);
      
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

      	List<String> nonleaf = new ArrayList<String>();
      	List<String> leaf = new ArrayList<String>();
      	BFS(path, _MAX_DEPTH, nonleaf, leaf);
      
      	// add child listener to this node and non-leaf node
      	// _zkClient.subscribeChildChanges(path, listenerTuple._zkChildListener);
      	for (String node : nonleaf)
      	{
      	  _zkClient.subscribeChildChanges(node, listenerTuple._zkChildListener);
      	}
      
      	// add data listener to leaf node, ensure leaf has version number at end
      	for (String node : leaf)
      	{
      	  if (!isValidLeaf(node))
      	  {
      	    _zkClient.subscribeChildChanges(node, listenerTuple._zkChildListener);
      	  }
      	  else
      	  {
      	    _zkClient.subscribeDataChanges(node, listenerTuple._zkDataListener);
      	  }
      	}
      }

    }
  }

  public void unsubscribeForRootPropertyChange(
      PropertyChangeListener<T> listener) throws PropertyStoreException
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
          List<String> nonleaf = new ArrayList<String>();
          List<String> leaf = new ArrayList<String>();
          BFS(path, _MAX_DEPTH, nonleaf, leaf);
        
          // remove child listener from non-leaf nodes
          for (String node : nonleaf)
          {
            _zkClient.unsubscribeChildChanges(node, listenerTuple._zkChildListener);
          }
        
          // remove data listener from leaf node, ensure leaf has version number at end
          for (String node : leaf)
          {
            if (!isValidLeaf(node))
            {
              _zkClient.unsubscribeChildChanges(node, listenerTuple._zkChildListener);
            }
            else
            {
              _zkClient.unsubscribeDataChanges(node, listenerTuple._zkDataListener);
            }
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

}
